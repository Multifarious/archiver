package io.ifar.archive.core.partitioner;

import java.util.HashMap;
import java.util.Map;

public class PerTopicDateRegexKafkaMessagePartitioner implements KafkaMessagePartitioner {

    HashMap<String, KafkaMessagePartitioner> partitionerMap = new HashMap<>();

    public PerTopicDateRegexKafkaMessagePartitioner(Map<String, DateRegexMessagePartitionerConfig> topicsConfig) {
        for(Map.Entry<String, DateRegexMessagePartitionerConfig> entry : topicsConfig.entrySet()) {
            partitionerMap.put(entry.getKey(), new DateRegexKafkaMessagePartitioner(
                    entry.getValue().dateExtractRegex, entry.getValue().getDateTimeFormatPattern()));
        }
    }

    @Override
    public ArchivePartitionData archivePartitionFor(String topic, int partition, byte[] rawMessagePayload) {
        KafkaMessagePartitioner partitioner = partitionerMap.get(topic);
        if(partitioner == null) throw new RuntimeException("Got message for unconfigured topic: " + topic);
        return partitioner.archivePartitionFor(topic, partition, rawMessagePayload);
    }
}
