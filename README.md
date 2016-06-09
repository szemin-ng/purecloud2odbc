# purecloud2odbc
Sample Go application that pulls previous interval queue statistics from PureCloud and writes it into an Microsoft Access database.

What's missing: Agent interval stats

Imports the following:
```
	"github.com/szemin-ng/purecloud"
	"github.com/szemin-ng/purecloud/analytics"
	"github.com/szemin-ng/purecloud/routing"
	"github.com/alexbrainman/odbc"
```

## Instructions
### PureCloud configuration
Create a new OAuth configuration with Client Credentials login and with a role that has access to queue statistics. Copy the Client ID and Client Secret from the OAuth configuration to the app's JSON configuration file.

### Microsoft Access database configuration
The application stores statistics into an Access database. Run ODBC Data Source Administrator on a Windows computer and create a new DSN, user or system, and point it to a version 4.x Access database. Version 4.x uses an extension of .MDB. When you choose Microsoft Access Driver for the DSN, you can create the Access database too. Key in the name of this DSN into the JSON config file.

### JSON config file
```
{
  "pureCloudRegion": "mypurecloud.com.au",
  "pureCloudClientId": <<from PureCloud OAuth configuration>>,
  "pureCloudClientSecret": <<from PureCloud OAuth configuration>>,
  "odbcDsn": <<the name of ODBC DSN that points to the Access database>>,
  "granularity": "PT30M",
  "queues": [
    "c2788c7e-c8c5-40ac-97d9-51c3b364479b","276148ba-40ad-4bad-a5a3-fedb9ddcbbb5"
  ]
}
```

For `queues`, get the `QueueID` from PureCloud Contact Center Queue administration screen. When the Queue's administration screen is opened, you can see the `QueueID` in the browser's URL, e.g., https://apps.mypurecloud.com.au/directory/#/admin/admin/organization/_queuesV2/97126d94-28fc-4178-b616-b009f466279a.

### Running the application
```
purecloud2odbc <JSONconfigfile>
```