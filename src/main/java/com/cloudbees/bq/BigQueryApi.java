package com.cloudbees.bq;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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
    }

    public void upload(File uploadFile){
        uploader.upload(uploadFile);
    }


    public void createTable(){
        try {
            Table table = new Table()
                    .setTableReference(new TableReference()
                            .setTableId(config.getTableId())
                            .setProjectId(config.getProjectId())
                            .setDatasetId(config.getDatasetId()))
                    .setSchema(config.getSchema());
            config.getBigQuery()
                    .tables()
                    .insert(config.getProjectId(), config.getDatasetId(), table)
                    .execute();
            LOGGER.info("Table {} created successfully.", table.getTableReference().getTableId());
        }catch (GoogleJsonResponseException e) {
            if(e.getStatusCode() == 409){
                LOGGER.error("Table: {} already exists. {}",config.getTableId(), e.getMessage());
            }else{
                LOGGER.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }catch (IOException e) {
            throw new RuntimeException("Failed to update schemaFile"+e.getMessage(), e);
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
