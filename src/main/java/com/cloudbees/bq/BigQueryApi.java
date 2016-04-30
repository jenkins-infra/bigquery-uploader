package com.cloudbees.bq;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.cloudbees.bq.BigQueryConfig.UPLOAD_HISTORY_TABLE_ID;

/**
 * @author Vivek Pandey
 */
public class BigQueryApi {

    private final BigQueryConfig config;
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryApi.class);
    private final Uploader uploader;

    public BigQueryApi(com.cloudbees.bq.BigQueryConfig config) {
        this.config = config;
        if(config.isStreamingUpload()){
            this.uploader = new StreamingUploader(config);
        }else {
            this.uploader = new BigQueryJobUploader(config);
        }

        if(getTable(UPLOAD_HISTORY_TABLE_ID) == null){
            createTable(UPLOAD_HISTORY_TABLE_ID, config.getUploadHistorySchema());
        }
    }

    public void upload(File uploadFile){
        uploader.upload(uploadFile);
    }


    public void createTable(String tableId, TableSchema schema){
        try {
            Table table = new Table()
                    .setTableReference(new TableReference()
                            .setTableId(tableId)
                            .setProjectId(config.getProjectId())
                            .setDatasetId(config.getDatasetId()))
                    .setSchema(schema);
            config.getBigQuery()
                    .tables()
                    .insert(config.getProjectId(), config.getDatasetId(), table)
                    .execute();
            LOGGER.info("Table {} created successfully.", table.getTableReference().getTableId());
        }catch (GoogleJsonResponseException e) {
            if(e.getStatusCode() == 409){
                LOGGER.error("Table: {} already exists. {}",tableId, e.getMessage());
            }else{
                LOGGER.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }catch (IOException e) {
            throw new RuntimeException("Failed to update schemaFile"+e.getMessage(), e);
        }
    }

    public Table getTable(String tableId){
        try {
            return config.getBigQuery().tables().get(config.getProjectId(), config.getDatasetId(), tableId).execute();
        } catch (HttpResponseException e){
          if(e.getStatusCode() == 404){
              LOGGER.error("Table "+tableId +" not found", e.getContent(), e);
              return null;
          }
          throw new RuntimeException("Failed to get table: "+tableId+": "+ e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get table: "+tableId+": "+ e.getMessage(), e);
        }
    }

    public void listTables(){
        try {
            TableList tableList = config.getBigQuery().tables().list(config.getProjectId(), config.getDatasetId()).execute();
            for(TableList.Tables table: tableList.getTables()){
                System.out.println(table.getTableReference().toPrettyString());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to list tables"+e.getMessage(), e);
        }
    }
}
