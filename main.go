package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/szemin-ng/purecloud"
	"github.com/szemin-ng/purecloud/analytics"
	"github.com/szemin-ng/purecloud/routing"

	_ "github.com/alexbrainman/odbc"
)

// AppConfig stores the application's config data
type AppConfig struct {
	PureCloudRegion       string   `json:"pureCloudRegion"`
	PureCloudClientID     string   `json:"pureCloudClientId"`
	PureCloudClientSecret string   `json:"pureCloudClientSecret"`
	OdbcDsn               string   `json:"odbcDsn"`
	Granularity           string   `json:"granularity"`
	Queues                []string `json:"queues"`
	Agents                []string `json:"agents"`
}

const configFile string = ""

//const configFile string = `c:\users\sze min\documents\go projects\src\purecloud2odbc\config.json`
const timeFormat string = "2006-01-02T15:04:05-0700"
const queueIntervalStatsTable string = "QueueIntervalStats"

var appConfig AppConfig // global app config
var supportedGranularity = map[string]time.Duration{"PT15M": time.Minute * 15, "PT30M": time.Minute * 30, "PT60M": time.Hour * 1, "PT1H": time.Hour * 1, "P1D": time.Hour * 24}
var supportedMediaType = []string{"voice", "chat", "email"}
var pureCloudToken purecloud.AccessToken
var db *sql.DB

func main() {
	var err error

	if err = loadAppConfig(configFile); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Connect to ODBC database
	if db, err = sql.Open("odbc", "DSN="+appConfig.OdbcDsn); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Prepare ODBC tables.  NO ERROR CHECKS (TODO)
	prepareDbTables()

	// Login to PureCloud using Client Credentials login
	if err = loginToPureCloud(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Cache a list of queue names to match with QueueIDs
	var queues map[string]string
	if queues, err = getPureCloudQueues(); err != nil {
		return
	}

	// Get queue interval statistics from PureCloud
	var resp purecloud.AggregateQueryResponse
	if resp, err = getPureCloudQueueStats(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	// Write queue interval stats to DB
	if err = writeQueueStatsToDb(resp, queues); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
}

// getPureCloudQueues returns a map of queueIDs and its corresponding queue names. Up to 1,000 active and inactive queues are returned.
func getPureCloudQueues() (queues map[string]string, err error) {
	var p = routing.GetQueueParams{PageSize: 1000, PageNumber: 1, Active: false}
	var queueList routing.QueueEntityListing

	queues = make(map[string]string)

	fmt.Printf("Retrieving list of configured queues...\n")
	if queueList, err = routing.GetListOfQueues(pureCloudToken, p); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	for _, queue := range queueList.Entities {
		queues[queue.ID] = queue.Name
	}
	fmt.Printf("Mapped %d queues\n", len(queues))

	return
}

func getPureCloudQueueStats() (resp purecloud.AggregateQueryResponse, err error) {
	// Format interval parameter for PureCloud's API call
	var startInterval, endInterval time.Time
	var y, d int
	var m time.Month
	var l *time.Location
	l, _ = time.LoadLocation("Local")

	if appConfig.Granularity == "P1D" {
		y, m, d = time.Now().Date()
		startInterval = time.Date(y, m, d, 0, 0, 0, 0, l)
		endInterval = startInterval.Add(time.Hour * 24)
	} else {
		startInterval = time.Now().Truncate(supportedGranularity[appConfig.Granularity])
		endInterval = startInterval.Add(supportedGranularity[appConfig.Granularity])
	}

	// Create the following query to use in API call
	/*{
	   "interval": "2016-06-08T00:00:00+08:00/2016-06-09T00:00:00+08:00",
	   "granularity": "P1D",
	   "groupBy": [ "mediaType", "queueId" ],
	   "filter": {
	       "type": "and",
	       "clauses": [
	           {
	              "type": "or",
	              "predicates": [
	                  { "dimension": "mediaType", "value": "chat" },
	                  { "dimension": "mediaType", "value": "..." },
	              ]
	           },
	           {
	              "type": "or",
	              "predicates": [
	                  { "dimension": "queueId", "value": "c2788c7e-c8c5-40ac-97d9-51c3b364479b" }
	                  { "dimension": "queueId", "value": "..." }
	              ]
	           }
	       ]
	   },
	   "flattenMultivaluedDimensions": true
	}*/

	var query = purecloud.AggregationQuery{
		Interval:    startInterval.Format(timeFormat) + "/" + endInterval.Format(timeFormat),
		Granularity: appConfig.Granularity,
		Filter: &purecloud.AnalyticsQueryFilter{
			Type: "and",
		},
		GroupBy: []string{"mediaType", "queueId"},
		FlattenMultiValuedDimensions: true,
	}

	// Add media type clause into the query
	var mediaTypeClause = purecloud.AnalyticsQueryClause{Type: "or"}
	for _, mediaType := range supportedMediaType {
		mediaTypeClause.Predicates = append(mediaTypeClause.Predicates, purecloud.AnalyticsQueryPredicate{Dimension: "mediaType", Value: mediaType})
	}

	// Add queue ID clause into the query
	var queueIDClause = purecloud.AnalyticsQueryClause{Type: "or"}
	for _, queueID := range appConfig.Queues {
		queueIDClause.Predicates = append(queueIDClause.Predicates, purecloud.AnalyticsQueryPredicate{Dimension: "queueId", Value: queueID})
	}

	// Append the clauses to the query. We do it last because Go's append returns a new copy of the slice
	query.Filter.Clauses = append(query.Filter.Clauses, mediaTypeClause)
	query.Filter.Clauses = append(query.Filter.Clauses, queueIDClause)

	if resp, err = analytics.QueryConversationAggregates(pureCloudToken, query); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	return
}

// loadAppConfig loads the config file for the app to run. If a configFile is passed in, e.g., C:\config.json, it uses that file. This is for testing purposes.
// In production, null string should be passed in so that it looks for the config file at os.Args[1]
func loadAppConfig(configFile string) (err error) {
	var f string

	// Config file supplied?
	if configFile == "" {
		if len(os.Args) < 2 {
			err = errors.New("Usage: %s configfile")
			return
		}
		f = os.Args[1]
	} else {
		f = configFile
	}

	// Read config file
	var b []byte
	if b, err = ioutil.ReadFile(f); err != nil {
		return
	}

	// Decode into AppConfig struct
	var d = json.NewDecoder(bytes.NewReader(b))
	if err = d.Decode(&appConfig); err != nil {
		return
	}

	// Validate granularity in config file
	if _, valid := supportedGranularity[appConfig.Granularity]; valid == false {
		err = errors.New("Invalid granularity. Use PT15M, PT30M, PT60M, PT1H or P1D")
		return
	}

	return
}

// loginToPureCloud logs into PureCloud using client credentials login
func loginToPureCloud() (err error) {
	fmt.Printf("Logging into PureCloud...\r")
	if pureCloudToken, err = purecloud.LoginWithClientCredentials(appConfig.PureCloudRegion, appConfig.PureCloudClientID, appConfig.PureCloudClientSecret); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Successfully logged in.\n")
	return
}

// prepareDbTables creates the table to hold queue interval stats. It does not do any error checks, just prints
// out any errors it receives
func prepareDbTables() {
	var err error

	// Drop table (for testing purposes)
	/*if _, err = db.Exec("DROP TABLE " + queueIntervalStatsTable); err != nil {
		fmt.Println(err)
	}*/

	// Create table, if error, just print it out and continue
	if _, err = db.Exec("CREATE TABLE " + queueIntervalStatsTable + " (QueueID VARCHAR, QueueName VARCHAR, MediaType VARCHAR, Interval DATETIME, " +
		"nError LONG, " +
		"nOffered LONG, " +
		"nOutboundAbandoned LONG, " +
		"nOutboundAttempted LONG, " +
		"nOutboundConnected LONG, " +
		"nTransferred LONG, " +
		"nOverSla LONG, " +
		"tAbandon DOUBLE, mtAbandon DOUBLE, nAbandon LONG, " +
		"tAcd DOUBLE, mtAcd DOUBLE, nAcd LONG, " +
		"tAcw DOUBLE, mtAcw DOUBLE, nAcw LONG, " +
		"tAgentResponseTime DOUBLE, mtAgentResponseTime DOUBLE, nAgentResponseTime LONG, " +
		"tAnswered DOUBLE, mtAnswered DOUBLE, nAnswered LONG, " +
		"tHandle DOUBLE, mtHandle DOUBLE, nHandle LONG, " +
		"tHeld DOUBLE, mtHeld DOUBLE, nHeld LONG, " +
		"tHeldComplete DOUBLE, mtHeldComplete DOUBLE, nHeldComplete LONG, " +
		"tIvr DOUBLE, mtIvr DOUBLE, nIvr LONG, " +
		"tTalk DOUBLE, mtTalk DOUBLE, nTalk LONG, " +
		"tTalkComplete DOUBLE, mtTalkComplete DOUBLE, nTalkComplete LONG, " +
		"tUserResponseTime DOUBLE, mtUserResponseTime DOUBLE, nUserResponseTime LONG)"); err != nil {
		fmt.Println(err)
	}

	// Create index, if error, just print it out and continue
	if _, err = db.Exec("CREATE INDEX QueueIndex ON " + queueIntervalStatsTable + " (QueueId, MediaType, Interval) WITH PRIMARY"); err != nil {
		fmt.Println(err)
	}
}

// queueIntervalExists checks if a queue interval exists in the database table, the primary key is a combination
// of QueueID, MediaType and Interval
func queueIntervalExists(queueID string, mediaType string, interval time.Time) (exists bool, err error) {
	var data string
	err = db.QueryRow("SELECT QueueID FROM "+queueIntervalStatsTable+" WHERE QueueID = ? AND MediaType = ? AND Interval = ?", queueID, mediaType, interval).Scan(&data)
	switch {
	case err == sql.ErrNoRows: // queue interval don't exist
		exists = false
		err = nil
		return
	case err != nil: // some other error
		return
	default:
		exists = true // queue interval exists
		err = nil
		return
	}
}

// writeQueueStatsToDb writes queue interval statistics in the response from /api/v2/analytics/conversations/aggregates/query into a database.
func writeQueueStatsToDb(dataset purecloud.AggregateQueryResponse, queueMap map[string]string) (err error) {
	var i int

	// Loop through results[]
	for _, result := range dataset.Results {
		var queueID, queueName, mediaType string // declare here so that it gets initialize for every iteration

		// Map queueID to a friendly queue name, replacing single quotes with '' for SQL statement compatibility. If name is not found, use queueID
		queueID = result.Group.QueueID
		queueName = strings.Replace(queueMap[queueID], "'", "''", -1)
		if queueName == "" {
			queueName = queueID
		}

		mediaType = result.Group.MediaType

		// Loop through results[].data[]
		for _, data := range result.Data {
			// declare variables here so that it gets initialized for every interval, each loop
			var interval time.Time
			var nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSLA int
			var nAbandon, nAcd, nAcw, nAgentResponseTime, nAnswered, nHandle, nHeld, nHeldComplete, nIvr, nTalk, nTalkComplete, nUserResponseTime int
			var tAbandon, mtAbandon, tAcd, mtAcd, tAcw, mtAcw, tAgentResponseTime, mtAgentResponseTime, tAnswered, mtAnswered, tHandle, mtHandle float64
			var tHeld, mtHeld, tHeldComplete, mtHeldComplete, tIvr, mtIvr, tTalk, mtTalk, tTalkComplete, mtTalkComplete, tUserResponseTime, mtUserResponseTime float64

			var s []string
			s = strings.Split(data.Interval, "/")
			if interval, err = time.Parse(time.RFC3339, s[0]); err != nil {
				panic(fmt.Sprintf("Could not parse interval %s to RFC3339 format", s[0]))
			}

			for _, metric := range data.Metrics {
				switch {
				case metric.Metric == "nError":
					nError = int(metric.Stats.Count)
				case metric.Metric == "nOffered":
					nOffered = int(metric.Stats.Count)
				case metric.Metric == "nOutboundAbandoned":
					nOutboundAbandoned = int(metric.Stats.Count)
				case metric.Metric == "nOutboundAttempted":
					nOutboundAttempted = int(metric.Stats.Count)
				case metric.Metric == "nOutboundConnected":
					nOutboundConnected = int(metric.Stats.Count)
				case metric.Metric == "nTransferred":
					nTransferred = int(metric.Stats.Count)
				case metric.Metric == "nOverSla":
					nOverSLA = int(metric.Stats.Count)
				case metric.Metric == "tAbandon":
					tAbandon = metric.Stats.Sum
					mtAbandon = metric.Stats.Max
					nAbandon = int(metric.Stats.Count)
				case metric.Metric == "tAcd":
					tAcd = metric.Stats.Sum
					mtAcd = metric.Stats.Max
					nAcd = int(metric.Stats.Count)
				case metric.Metric == "tAcw":
					tAcw = metric.Stats.Sum
					mtAcw = metric.Stats.Max
					nAcw = int(metric.Stats.Count)
				case metric.Metric == "tAgentResponseTime":
					tAgentResponseTime = metric.Stats.Sum
					mtAgentResponseTime = metric.Stats.Max
					nAgentResponseTime = int(metric.Stats.Count)
				case metric.Metric == "tAnswered":
					tAnswered = metric.Stats.Sum
					mtAnswered = metric.Stats.Max
					nAnswered = int(metric.Stats.Count)
				case metric.Metric == "tHandle":
					tHandle = metric.Stats.Sum
					mtHandle = metric.Stats.Max
					nHandle = int(metric.Stats.Count)
				case metric.Metric == "tHeld":
					tHeld = metric.Stats.Sum
					mtHeld = metric.Stats.Max
					nHeld = int(metric.Stats.Count)
				case metric.Metric == "tHeldComplete":
					tHeldComplete = metric.Stats.Sum
					mtHeldComplete = metric.Stats.Max
					nHeldComplete = int(metric.Stats.Count)
				case metric.Metric == "tIvr":
					tIvr = metric.Stats.Sum
					mtIvr = metric.Stats.Max
					nIvr = int(metric.Stats.Count)
				case metric.Metric == "tTalk":
					tTalk = metric.Stats.Sum
					mtTalk = metric.Stats.Max
					nTalk = int(metric.Stats.Count)
				case metric.Metric == "tTalkComplete":
					tTalkComplete = metric.Stats.Sum
					mtTalkComplete = metric.Stats.Max
					nTalkComplete = int(metric.Stats.Count)
				case metric.Metric == "tUserResponseTime":
					tUserResponseTime = metric.Stats.Sum
					mtUserResponseTime = metric.Stats.Max
					nUserResponseTime = int(metric.Stats.Count)
				default:
					panic(fmt.Sprintf("Unrecognized metric %s", metric.Metric))
				}
			}

			// If queue interval exists in table, update the existing interval, we don't want to violate the primary key
			// If queue interval don't exist in table, insert a new interval
			var exists bool
			var t string
			if exists, err = queueIntervalExists(queueID, mediaType, interval); err != nil {
				return
			}
			if exists == true {
				fmt.Printf("Updating record %d\r", i+1)
				t = fmt.Sprintf("UPDATE "+queueIntervalStatsTable+" SET "+
					"nError = %d, nOffered = %d, nOutboundAbandoned = %d, nOutboundAttempted = %d, nOutboundConnected = %d, nTransferred = %d, nOverSla = %d, "+
					"tAbandon = %f, mtAbandon = %f, nAbandon = %d, tAcd = %f, mtAcd = %f, nAcd = %d, tAcw = %f, mtAcw = %f, nAcw = %d, tAgentResponseTime = %f, mtAgentResponseTime = %f, nAgentResponseTime = %d, "+
					"tAnswered = %f, mtAnswered = %f, nAnswered = %d, tHandle = %f, mtHandle = %f, nHandle = %d, tHeld = %f, mtHeld = %f, nHeld = %d, tHeldComplete = %f, mtHeldComplete = %f, nHeldComplete = %d, "+
					"tIvr = %f, mtIvr = %f, nIvr = %d, tTalk = %f, mtTalk = %f, nTalk = %d, tTalkComplete = %f, mtTalkComplete = %f, nTalkComplete = %d, tUserResponseTime = %f, mtUserResponseTime = %f, nUserResponseTime = %d "+
					"WHERE QueueId = '%s' AND MediaType = '%s' AND Interval = {ts '%s'}",
					nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSLA,
					tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime,
					tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete,
					tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime,
					queueID, mediaType, interval.Format("2006-01-02 15:04:05"))
			} else {
				fmt.Printf("Inserting record %d\r", i+1)
				t = fmt.Sprintf("INSERT INTO "+queueIntervalStatsTable+" ("+
					"QueueID, QueueName, MediaType, Interval, nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSla, "+
					"tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime, "+
					"tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete, "+
					"tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime) "+
					"VALUES ('%s', '%s', '%s', {ts '%s'}, %d, %d, %d, %d, %d, %d, %d, "+
					"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d, "+
					"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d, "+
					"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d)",
					queueID, queueName, mediaType, interval.Format("2006-01-02 15:04:05"), nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, nOverSLA,
					tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime,
					tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete,
					tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime)
			}

			if _, err = db.Exec(t); err != nil {
				return
			}

			i++
		}
	}
	fmt.Printf("\nWriting done\n")
	return
}
