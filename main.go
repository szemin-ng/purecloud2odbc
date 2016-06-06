package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/szemin-ng/purecloud"
	"github.com/szemin-ng/purecloud/analytics"
	"github.com/szemin-ng/purecloud/routing"

	_ "github.com/alexbrainman/odbc"
)

const clientID string = "62efbdcb-7e92-4f15-bf90-f6bcef9bcd38"
const clientSecret string = "ad6xRwuthCdjKu5uTyJ5AhST4n8pEqwMromZeMbLlko"

func main() {
	var err error

	var db *sql.DB
	if db, err = sql.Open("odbc", "DSN=PureCloudStats"); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	var token purecloud.AccessToken
	if token, err = loginToPureCloud(purecloud.AustraliaRegion, clientID, clientSecret); err != nil {
		return
	}

	var queues map[string]string
	if queues, err = getPureCloudQueues(token); err != nil {
		return
	}

	var query = purecloud.AggregationQuery{
		Interval:    "2016-01-01T00:00:00.000+08:00/2016-05-31T23:59:59.000+08:00",
		Granularity: "PT30M",
		GroupBy:     []string{"mediaType", "queueId"},
		FlattenMultiValuedDimensions: true,
	}

	var resp purecloud.AggregateQueryResponse
	if resp, err = analytics.QueryConversationAggregates(token, query); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	if err = writeToDatabase(db, resp, queues); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
}

// getPureCloudQueues returns a map of queueIDs and its corresponding queue names. Up to 1,000 active and inactive queues are returned.
func getPureCloudQueues(token purecloud.AccessToken) (queues map[string]string, err error) {
	var p = routing.GetQueueParams{PageSize: 1000, PageNumber: 1, Active: false}
	var queueList routing.QueueEntityListing

	queues = make(map[string]string)

	fmt.Printf("Retrieving list of configured queues...\n")
	if queueList, err = routing.GetListOfQueues(token, p); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	for _, queue := range queueList.Entities {
		queues[queue.ID] = queue.Name
	}
	fmt.Printf("Mapped %d queues\n", len(queues))

	return
}

// writeToDatabase writes queue interval statistics in the response from /api/v2/analytics/conversations/aggregates/query into a database.
func writeToDatabase(db *sql.DB, dataset purecloud.AggregateQueryResponse, queueMap map[string]string) (err error) {
	var i int

	for _, result := range dataset.Results {
		var queueID, queueName, mediaType string // declare here so that it gets initialize for every iteration

		// If queueID is empty, then this interaction did not enter a queue and we ignore the stats
		queueID = result.Group.QueueID
		if queueID == "" {
			continue
		}

		// Map queueID to a friendly queue name, replacing single quotes with '' for SQL statement compatibility. If name is not found, use queueID
		queueName = strings.Replace(queueMap[queueID], "'", "''", -1)
		if queueName == "" {
			queueName = queueID
		}

		mediaType = result.Group.MediaType
		if mediaType == "" {
			panic("mediaType not found in results")
		}

		for _, data := range result.Data {
			fmt.Printf("Writing record %d\r", i+1)

			// declare variables here so that it gets initialized for every interval
			var interval time.Time
			var nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred int
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

			var t string
			t = fmt.Sprintf("INSERT INTO QueueIntervalStats ("+
				"QueueID, QueueName, MediaType, Interval, nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred, "+
				"tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime, "+
				"tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete, "+
				"tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime) "+
				"VALUES ('%s', '%s', '%s', {ts '%s'}, %d, %d, %d, %d, %d, %d, "+
				"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d, "+
				"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d, "+
				"%f, %f, %d, %f, %f, %d, %f, %f, %d, %f, %f, %d)",
				queueID, queueName, mediaType, interval.Format("2006-01-02 15:04:05"), nError, nOffered, nOutboundAbandoned, nOutboundAttempted, nOutboundConnected, nTransferred,
				tAbandon, mtAbandon, nAbandon, tAcd, mtAcd, nAcd, tAcw, mtAcw, nAcw, tAgentResponseTime, mtAgentResponseTime, nAgentResponseTime,
				tAnswered, mtAnswered, nAnswered, tHandle, mtHandle, nHandle, tHeld, mtHeld, nHeld, tHeldComplete, mtHeldComplete, nHeldComplete,
				tIvr, mtIvr, nIvr, tTalk, mtTalk, nTalk, tTalkComplete, mtTalkComplete, nTalkComplete, tUserResponseTime, mtUserResponseTime, nUserResponseTime)

			// more checks on results of insert needed
			if _, err = db.Exec(t); err != nil {
				return
			}

			i++
		}
	}
	fmt.Printf("\nWriting done\n")
	return
}

// loginToPureCloud logs into PureCloud using client credentials login and returns an AccessToken for further API calls
func loginToPureCloud(region string, clientID string, clientSecret string) (token purecloud.AccessToken, err error) {
	fmt.Printf("Logging in...\r")
	if token, err = purecloud.LoginWithClientCredentials(region, clientID, clientSecret); err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Successfully logged in.\n")
	return
}
