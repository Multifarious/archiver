package io.ifar.archive.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Duration;

import javax.validation.constraints.NotNull;

public class TopicConfiguration {

    @JsonProperty
    @NotNull
    private String bucket;

    @JsonProperty
    @NotNull
    private Duration maxBatchDuration = Duration.hours(1);

    TopicConfiguration() {
    }

    public TopicConfiguration(String bucket) {
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    public Duration getMaxBatchDuration() {
        return maxBatchDuration;
    }
}
