package io.ifar.archive.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jackson.JsonSnakeCase;

@JsonSnakeCase
public class NodeData {
    @JsonProperty
    private String topic;
    @JsonProperty
    private String partition;
    @JsonProperty
    private String offset;

    public NodeData() {}

    public NodeData(String topic, String partition, String offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    @JsonIgnore
    public int partition() {
        return Integer.parseInt(partition);
    }
}
