/**
 * Jenkinsfile to automate uploading of census and extension data to BigQuery
 *
 * Job setup requirement:
 *  1. install credential-binding plugin
 *  2. google service account with read/write access to 'jenkins-user-stats' project
 *  3. Create Google JSON private key with scope: https://www.googleapis.com/auth/bigquery. This scope manages data
 *    in big query
 *  4. Create Jenkins credential for 'secret text'
 *  5. The pipeline job must be configured for required credential parameter with GOOGLE_OAUTH_CREDENTIAL key and value
 *    of the credential created in step 4.
 *
 *
 * @author Vivek Pandey
 */
def nodeLabel = 'docker'
dockerImage = 'vivekpandey/java8-ruby:v1.1'

/**
 * Build https://github.com/jenkins-infra/bigquery-uploader.git
 */
stage 'build'
node(nodeLabel) {
    dockerRun {
        git url: 'https://github.com/jenkins-infra/bigquery-uploader.git'
        sh "rm -f *.json || true && rm *.bq || true"
        sh "mvn clean install"
    }
}

/**
 * Upload extensions index and census data in parallel
 */
stage 'upload'
parallel(
        'uploadExtension':{
            node(nodeLabel) {
                upload("plugin_extensions",'jenkins-extensions-schema.json', 'ext.bq','WRITE_TRUNCATE') {
                    recordFile ->
                        // Download latest extensionPoint data
                        downloadExtensionIndex()

                        // Prepare downloaded raw data for BigQuery
                        prepareExtensionData('extension-points.json', recordFile)
                }
            }
        }
//        , 'uploadCensus':{
//             node(nodeLabel) {
//                 upload("plugin_extensions",'jenkins-extensions-schema.json', 'census.bq','WRITE_TRUNCATE') {
//                     recordFile ->
//                         downloadCensus()
//                         prepareCensusData('aceess_log', recordFile);
//                 }
//             }
//        }
)

// Archive json and bq files
stage 'archive'
node(nodeLabel) {
    dockerRun {
        archive '*.json, *.bq'
    }
}


// Helper functions

/**
 * Uploads prepared big query data
 *
 * @param tableId BigQuery table ID
 * @param schema BigQuery schema
 * @param recordFile record file that is to be uploaded
 * @param writeDisposition If a table exists, how the uploaded data to be stored in it
 * @param preparer closure that does prepare raw data in to big query friend record file
 *
 */
def upload(String tableId, String schema, String recordFile, String writeDisposition="WRITE_APPEND", Closure preparer) {
    // GOOGLE_OAUTH_CREDENTIAL is build parameter set to 'secrect text' type credential
    String key = credential(GOOGLE_OAUTH_CREDENTIAL)
    preparer.call(recordFile)
    dockerRun {
        withEnv(["GOOGLE_JSON_KEY=$key"]){
            sh "java -jar ./target/bigquery-uploader-1.0-SNAPSHOT-all.jar \
                    -projectId jenkins-user-stats \
                    -datasetId jenkinsstats \
                    -tableId $tableId \
                    -bqFile  $recordFile  \
                    -schemaFile ./schema/$schema \
                    -createTable \
                    -writeDisposition $writeDisposition"
        }
    }
}

/**
 * Gets google OAuth JSON private key credential.
 *
 * The credential must be stored as secret text using Credential Binding Plugin and made available using Parameterized
 * build with parameter 'GOOGLE_OAUTH_CREDENTIAL' pointing to the secret text credential
 *
 * @param name ID of the credential
 * @return
 */
def credential(name){
    withCredentials([[$class: 'StringBinding', credentialsId: name, variable: 'googleOAuthKey']]) {
        return "${env.googleOAuthKey}"
    }
}

def downloadExtensionIndex() {
    dockerRun {
        sh "wget https://ci.jenkins-ci.org/view/Infrastructure/job/infra_extension-indexer/lastBuild/artifact/extension-points.json"
    }
}

/**
 * Prepare raw extension data for upload to BigQuery
 * @param src source raw data file
 * @param dest destination file where bigquery record is stored
 *
 */
def prepareExtensionData(String src, String dest) {
    dockerRun {
        sh "./scripts/extensions_data_prepare.rb $src $dest"
    }
}

//TODO: How to get dailiy uncrypted anonymized census data?
def downloadCensus() {

}
/**
 * Prepare anonymized unencrypted census data for upload to BigQuery
 * @param src source raw data file
 * @param dest destination file where bigquery record is stored
 *
 */
def prepareCensusData(String src, String dest) {
    dockerRun {
        sh "./scripts/usage_data_prepare.rb $src $dest"
    }
}


def dockerRun(Closure c) {
    /* This requires the Timestamper plugin to be installed on the Jenkins */
    wrap([$class: 'TimestamperBuildWrapper']) {
        docker.image(dockerImage).inside('-u 0:0 -v /Users/vivek/.m2/repository:/root/.m2/repository', c)
    }
}