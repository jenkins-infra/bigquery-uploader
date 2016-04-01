package com.cloudbees.bq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Vivek Pandey
 */
public class StreamingUploader extends Uploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryApi.class);

    public StreamingUploader(BigQueryConfig config) {
        super(config);
    }

    public void upload(File uploadFile) {
        final Iterator<String> rows;
        try {

            rows = FileUtils.lineIterator(uploadFile, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Failed to read BigQuery upload file: "+uploadFile.getAbsolutePath(), e);
        }

        Iterator<TableDataInsertAllResponse> responses = new Iterator<TableDataInsertAllResponse>() {
            /**
             * Check whether there is another row to stream.
             *
             * @return True if there is another row in the stream
             */
            public boolean hasNext() {
                return rows.hasNext();
            }

            /**
             * Insert the next row, and return the response.
             *
             * @return Next page of data
             */
            public TableDataInsertAllResponse next() {
                try {
                    Map<String,Object> m = om.readValue(rows.next(), new TypeReference<Map<String,Object>>() {});
                    if(m != null){
                        TableDataInsertAllRequest.Rows r = new TableDataInsertAllRequest.Rows();
                        if(config.getInsertIdField() != null){
                            String insertionId = (String) m.get(config.getInsertIdField());
                            if(insertionId != null){
                                r.setInsertId(insertionId);
                            }
                        }
                        r.setJson(m);
                        return config.getBigQuery().tabledata().insertAll(
                                config.getProjectId(),
                                config.getDatasetId(),
                                config.getTableId(),
                                new TableDataInsertAllRequest()
                                        .setIgnoreUnknownValues(true)
                                        .setTemplateSuffix(config.getTemplateSuffix())
                                        .setRows(Collections.singletonList(r)))
                                .execute();
                    }
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                return null;
            }

            public void remove() {
                this.next();
            }

        };

        while (responses.hasNext()) {
            TableDataInsertAllResponse response = responses.next();
            if(response == null){
                continue;
            }
            if(response.getInsertErrors() != null && !response.getInsertErrors().isEmpty()){
                LOGGER.error(response.toString());
            }else {
                LOGGER.debug(response.toString());
            }
        }
        LOGGER.info("Streaming upload of table {} completed", config.getTableId());
    }
}
