package com.cloudbees.bq;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @author Vivek Pandey
 */
public abstract class Uploader {
    protected final ObjectMapper om;
    protected final BigQueryConfig config;
    protected final SimpleDateFormat sdf;

    public Uploader(BigQueryConfig config) {
        this.config = config;
        this.om = new ObjectMapper();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS z");
        this.sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        om.setDateFormat(sdf);
    }

    abstract void upload(File content);
}
