package io.ifar.archive;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.ifar.archive.core.TopicConfiguration;
import io.ifar.archive.core.partitioner.DateRegexMessagePartitionerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

public class ArchiveConfiguration extends Configuration {
    static final Logger LOG = LoggerFactory.getLogger(ArchiveConfiguration.class);

    @JsonProperty
    String serviceName = "archive-service";

    @JsonProperty
    String zkRoot = "io/ifar/archive";

    @JsonProperty
    @NotNull
    String seedBrokers;     // same format as metadata.broker.list for producer

    @JsonProperty
    @NotNull
    String zookeeperServerstring;

    @JsonProperty
    @NotNull
    S3Configuration s3Configuration = new S3Configuration();

    @JsonProperty
    String defaultBucket;

    @JsonProperty
    @NotNull
    Map<String, TopicConfiguration> topicConfiguration = new HashMap<>();

    @JsonProperty
    private String customKafkaMessagePartitionerClass;

    @JsonProperty
    private Map<String, DateRegexMessagePartitionerConfig> kafkaMessagePartitionerConfig;

    @JsonProperty
    int maxNumParallelWorkers = 16;

    public String getServiceName() {
        return serviceName;
    }

    public S3Configuration getS3Configuration() {
        return s3Configuration;
    }

    public String getDefaultBucket() {
        return defaultBucket;
    }

    public Map<String, TopicConfiguration> getTopicConfiguration() {
        return topicConfiguration;
    }

    public String getZkRoot() { return zkRoot; }

    public String getSeedBrokers() { return seedBrokers; }

    public String getZookeeperServerstring() { return zookeeperServerstring; }

    public String getCustomKafkaMessagePartitionerClass() {
        return customKafkaMessagePartitionerClass;
    }

    public Map<String, DateRegexMessagePartitionerConfig> getKafkaMessagePartitionerConfig() {
        return kafkaMessagePartitionerConfig;
    }

    public int getMaxNumParallelWorkers() {
        return maxNumParallelWorkers;
    }
}
