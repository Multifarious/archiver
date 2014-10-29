package io.ifar.archive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3Configuration {
    @JsonProperty
    private String accessKeyId;

    @JsonProperty
    private String secretAccessKey;

    @JsonProperty
    private String bucket;

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getBucket() {
        return bucket;
    }

}

