package com.cloudbees.bq;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * @author Vivek Pandey
 */
public class UploadHistory {
    @JsonProperty("status")
    public Status status;

    @JsonProperty("timestamp")
    public Date timestamp;

    @JsonProperty("file")
    public UploadFile uploadFile;

    public static class UploadFile{
        @JsonProperty("name")
        public String name;

        @JsonProperty("sizeInMB")
        public float sizeInMB;

        @JsonProperty("timestamp")
        public Date timestamp;

        @JsonProperty("type")
        public Type type;
    }

    enum Type {CENSUS, EXTENSION}

    enum Status {STARTED, COMPLETED, FAILED}
}
