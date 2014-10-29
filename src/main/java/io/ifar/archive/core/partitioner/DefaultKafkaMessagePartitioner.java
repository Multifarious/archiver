package io.ifar.archive.core.partitioner;

import java.util.Date;

/**
 * Partitions by Kafka partition number and system time.
 */
public class DefaultKafkaMessagePartitioner implements KafkaMessagePartitioner {
    @Override
    public ArchivePartitionData archivePartitionFor(String topic, int partition, byte[] rawMessagePayload) {
        String message = new String(rawMessagePayload);
        return new ArchivePartitionData(message, String.format("%d", partition), new Date());
    }
}
