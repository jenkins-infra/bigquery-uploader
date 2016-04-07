package com.cloudbees.bq;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * @author Vivek Pandey
 */
public class BigQueryConfig {
    public static final String WRITE_APPEND="WRITE_APPEND";
    public static final String WRITE_EMPTY="WRITE_EMPTY";
    public static final String WRITE_TRUNCATE="WRITE_TRUNCATE";

    private final String projectId;
    private final String datasetId;
    private final String tableId;
    private final Bigquery bigQuery;
    private String templateSuffix;
    private String insertIdField;
    private boolean createTable;
    private TableSchema schema;
    private int pollingIntervalInSec;
    private boolean streamingUpload;
    private String writeDisposition;

    private BigQueryConfig(String projectId, String datasetId, String tableId, File credentialFile) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableId = tableId;
        try {
            this.bigQuery = createAuthorizedClient(credentialFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to authenticate with Google BigQuery: "+e.getMessage(), e);
        }
    }

    public String getProjectId() {
        return projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getTableId() {
        return tableId;
    }

    public Bigquery getBigQuery() {
        return bigQuery;
    }

    public String getTemplateSuffix() {
        return templateSuffix;
    }

    public String getInsertIdField() {
        return insertIdField;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public boolean isStreamingUpload() {
        return streamingUpload;
    }

    public int getPollingIntervalInSec() {
        return pollingIntervalInSec;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public String getWriteDisposition() {
        return writeDisposition;
    }

    public static final class Builder{
        private final BigQueryConfig config;


        public Builder(String projectId, String datasetId, String tableId, File creadentialFile) {
            this.config = new BigQueryConfig(projectId,datasetId,tableId,creadentialFile);
        }

        public Builder templateSuffix(String templateSuffix){
            this.config.templateSuffix = templateSuffix;
            return this;
        }

        public Builder insertIdField(String insertIdField){
            this.config.insertIdField = insertIdField;
            return this;
        }

        public Builder schema(File schema){
            if(schema != null) {
                try {
                    config.schema = config.getBigQuery()
                            .getJsonFactory()
                            .fromInputStream(new FileInputStream(schema), TableSchema.class);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse Google BigQuery schemaFile: " + e.getMessage(), e);
                }
            }
            return this;
        }

        public Builder createTable(boolean createTable){
            config.createTable = createTable;
            return this;
        }

        public Builder pollingIntervalInSec(int interval){
            this.config.pollingIntervalInSec = interval;
            return this;
        }

        public Builder streamingUpload(boolean streamingUpload){
            config.streamingUpload = streamingUpload;
            return this;
        }

        public Builder writeDisposition(String writeDisposition){
            if(writeDisposition == null){
                writeDisposition = WRITE_APPEND;
            }else if(!writeDisposition.equals(WRITE_APPEND)
                    && !writeDisposition.equals(WRITE_EMPTY)
                    && !writeDisposition.equals(WRITE_TRUNCATE)){
                throw new IllegalArgumentException(String.format("-writeDisposition must be one of %s (default), %s or %s", WRITE_APPEND, WRITE_EMPTY, WRITE_TRUNCATE));
            }
            config.writeDisposition = writeDisposition;
            return this;
        }

        public BigQueryConfig build(){
            return config;
        }
    }

    private  Bigquery createAuthorizedClient(File credentialFile) throws IOException {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        com.google.api.client.json.JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential=null;
        if(credentialFile == null) {
            if (System.getenv().get("GOOGLE_JSON_KEY") != null) {
                String key = System.getenv().get("GOOGLE_JSON_KEY");
                credential = GoogleCredential.fromStream(new ByteArrayInputStream(key.getBytes()));
            } else if(System.getenv("GOOGLE_APPLICATION_CREDENTIALS") != null){
                credential = GoogleCredential.getApplicationDefault();
            }
        }else {
            credential = GoogleCredential.fromStream(new FileInputStream(credentialFile));
        }
        if(credential == null){
            throw new RuntimeException("No google credentials found");
        }

        // Depending on the environment that provides the default credentials (e.g. Compute Engine, BigQueryApi
        // Engine), the credentials may require us to specify the scopes we need explicitly.
        // Check for this case, and inject the Bigquery scope if required.
        if (credential.createScopedRequired()) {
            Collection<String> bigqueryScopes = ImmutableList.of(BigqueryScopes.BIGQUERY);
            credential = credential.createScoped(bigqueryScopes);
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Jenkins usage uploadFile").build();
    }
}
