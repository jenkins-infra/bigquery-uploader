package com.cloudbees.bq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static com.cloudbees.bq.BigQueryConfig.UPLOAD_HISTORY_TABLE_ID;

/**
 * @author Vivek Pandey
 */
public abstract class Uploader {
    protected final ObjectMapper om;
    protected final BigQueryConfig config;
    protected final SimpleDateFormat sdf;

    private static final Logger LOGGER = LoggerFactory.getLogger(Uploader.class);


    public Uploader(BigQueryConfig config) {
        this.config = config;
        this.om = new ObjectMapper();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS z");
        this.sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        om.setDateFormat(sdf);
    }

    protected void recordUploadStatus(File uploadFile, UploadHistory.Status status){
        UploadHistory uploadHistory = new UploadHistory();
        uploadHistory.uploadFile = new UploadHistory.UploadFile();
        uploadHistory.uploadFile.name = uploadFile.getName();
        uploadHistory.uploadFile.sizeInMB = (float) (Math.round((uploadFile.length()/1000000.00)*100.0)/100.0);
        uploadHistory.uploadFile.timestamp = getFileTimestamp(uploadFile);
        uploadHistory.uploadFile.type = config.getUploadType();
        uploadHistory.timestamp = new Date();
        uploadHistory.status = status;
        try {
            LOGGER.info(String.format("Uploading to table: %s, status: %s", UPLOAD_HISTORY_TABLE_ID, status));
            doUpload(UPLOAD_HISTORY_TABLE_ID, new ByteArrayContent("application/octet-stream", om.writeValueAsString(uploadHistory).getBytes("UTF-8")));
        } catch (UnsupportedEncodingException | JsonProcessingException e) {
            LOGGER.error("JSON serialization failed: "+e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Date getFileTimestamp(File uploadFile){
        if(config.getUploadType().equals("census")){
            SimpleDateFormat df = new SimpleDateFormat("yyyymmdd");
            String[] tokens = uploadFile.getName().split(".");
            try {
                return df.parse(tokens[tokens.length - 2]);
            } catch (ParseException e) {
                LOGGER.error("Failed to parse census timestamp from filename: "+uploadFile.getAbsolutePath(),e);
                throw new RuntimeException(e);
            }
        }else{
            return new Date();
        }
    }

    public final void upload(File content) {
        boolean error=false;
        boolean started = false;
        boolean failed = false;
        boolean completed = false;
        try{
            if(config.getUploadType() == UploadHistory.Type.CENSUS){
                //check if this file is already uploaded
                QueryRequest queryRequest = new QueryRequest();
                queryRequest.setQuery(String.format("select status from [%s:%s.%s] where file.name='%s'", config.getProjectId(),
                        config.getDatasetId(), UPLOAD_HISTORY_TABLE_ID, content.getName()));
                QueryResponse response = config.getBigQuery().jobs().query(config.getProjectId(), queryRequest).execute();
                if(response.getRows() != null) {
                    for (TableRow row : response.getRows()) {
                        for (TableCell cell : row.getF()) {
                            if (cell.getV() == null) {
                                continue;
                            }
                            UploadHistory.Status s = UploadHistory.Status.valueOf((String) cell.getV());
                            if (s == UploadHistory.Status.STARTED) {
                                started = true;
                            } else if (s == UploadHistory.Status.FAILED) {
                                failed = true;
                            } else if (s == UploadHistory.Status.COMPLETED) {
                                completed = true;
                            }
                        }
                    }
                    if (started || failed || completed) {
                        LOGGER.error("There was previous attempt to upload file: " + content.getName());
                        return;
                    }
                }
            }
            started = true;
            recordUploadStatus(content, UploadHistory.Status.STARTED);

            LOGGER.info("Uploading " + config.getUploadType() + " data, file: "+ content.getName());
            doUpload(config.getTableId(), content);

            completed = true;
            recordUploadStatus(content, UploadHistory.Status.COMPLETED);
        }catch (Exception e){
            error = true;
            LOGGER.error("Upload failed: "+e.getMessage(), e);
            throw new RuntimeException(e);
        }finally {
            if(started && !completed && error){
                recordUploadStatus(content, UploadHistory.Status.FAILED);
            }
        }
    }

    abstract void doUpload(String tableId, File content);
    abstract void doUpload(String tableId, AbstractInputStreamContent content);
}
