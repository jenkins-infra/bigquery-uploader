package com.cloudbees.bq;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.FileContent;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author Vivek Pandey
 */
public class BigQueryJobUploader extends Uploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryJobUploader.class);

    public BigQueryJobUploader(BigQueryConfig config) {
        super(config);
    }

    @Override
    void doUpload(String tableId, File content) {
        FileContent file = new FileContent("application/octet-stream", content);
        doUpload(tableId, file);
    }

    void doUpload(String tableId, AbstractInputStreamContent content) {
        try {
            Table t = config.getBigQuery().tables().get(config.getProjectId(),config.getDatasetId(),tableId).execute();

            Job job = new Job();

            JobConfiguration jobConfig = new JobConfiguration();
            JobConfigurationLoad configLoad = new JobConfigurationLoad();
            configLoad.setSchema(t.getSchema());
            configLoad.setSourceFormat("NEWLINE_DELIMITED_JSON");
            configLoad.setDestinationTable(t.getTableReference());

            configLoad.setEncoding("UTF-8");
            configLoad.setCreateDisposition("CREATE_IF_NEEDED");
            configLoad.setWriteDisposition(config.getWriteDisposition());//WRITE_APPEND is default
            configLoad.setIgnoreUnknownValues(true);
            jobConfig.setLoad(configLoad);

            job.setConfiguration(jobConfig);

            Bigquery.Jobs.Insert insert;
            insert = config.getBigQuery().jobs().insert(config.getProjectId(), job, content);

            Job j = insert.execute();
            if (!j.getStatus().getState().equals("DONE")) {
                pollJob(j.getJobReference().getJobId(), config.getPollingIntervalInSec());
            }else{
                LOGGER.error(j.getStatus().toString());
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    private Job pollJob(String jobId, int interval) throws IOException, InterruptedException {
        Bigquery.Jobs.Get request = config.getBigQuery().jobs().get(config.getProjectId(), jobId);
        Job job = request.execute();
        while (!job.getStatus().getState().equals("DONE")) {
            LOGGER.debug("Job is "
                    + job.getStatus().getState()
                    + " waiting " + interval + " seconds...");
            Thread.sleep(interval*1000);
            job = request.execute();
        }
        if(job.getStatus().getErrorResult() != null){
            LOGGER.error(job.getStatus().getErrorResult().getMessage()+". Status: "+job.getStatus().getState());
            for(ErrorProto e : job.getStatus().getErrors()){
                LOGGER.error(e.getMessage());
            }
        }else{
            LOGGER.info("Upload finished successfully.");
        }

        return job;
    }
}
