# Google BigQuery Uploader

Uploads JSON record (each JSON object sperated by new line) content from file to Google BigQuery

## Build
    $ mvn clean install
    
## Usage
    $ java -jar target/bigquery-uploader-1.0-SNAPSHOT-all.jar -h
     -bqFile FILE                 : BigQuery record file (JSON object separated by
                                    new line)
     -createTable                 : Create new table using the given -tableId and
                                    -schemaFile (default: false)
     -credentialFile FILE         : BigQuery JSON credential file
     -datasetId VAL               : BigQuery datasetId (Required)
     -h (-help)                   : Print help message (default: true)
     -insertIdField VAL           : Top level JSON field to use for insertId
                                    (streaming upload only)
     -pollingInterval N           : Submitted job polling interval(in seconds)
                                    (default: 1)
     -projectId VAL               : BigQuery projectId (Required)
     -schemaFile SCHEMA_JSON_FILE : Create BigQuery table using provided schama
     -streamingUpload             : Create new table using streaming upload
                                    (default: false)
     -tableId VAL                 : BigQuery tableId (Required)
     -templateSuffix VAL          : Template suffix to be used with this upload
                                    (streaming upload only)

### Authentication

Create credential (JSON private key) using your service account. Refer to: https://console.cloud.google.com/apis/credentials. 
Once you have the JSON private key, you can use any of the following options to setup credential to be used by uploader.

* Use -credentialFile to provide path to your Google BigQuery JSON private key
* Set GOOGLE_APPLICATION_CREDENTIALS to the path to the JSON private key file
* Set GOOGLE_JSON_KEY to the content of JSON private key file

### Create table (if does not exist) and upload data

    java -jar target/bigquery-uploader-1.0-SNAPSHOT-all.jar \
        -projectId PROJECTID \
        -datasetId DATASETID \
        -tableId TABLEID \
        -credentialFile PATH_TO_GOOGLE_API_JSON_PRIVATE_KEY \
        -bqFile PATH_TO_BQ_CONTENT_FILE \
        -schemaFile PATH_TO_SCHEMA \
        -createTable

### Upload data using streaming (slow)
    java -jar target/bigquery-uploader-1.0-SNAPSHOT-all.jar \
        -projectId PROJECTID \
        -datasetId DATASETID \
        -tableId TABLEID \
        -credentialFile PATH_TO_GOOGLE_API_JSON_PRIVATE_KEY \
        -bqFile PATH_TO_BQ_CONTENT_FILE \
        -schemaFile PATH_TO_SCHEMA \
        -createTable \
        -streamingUpload
        
        