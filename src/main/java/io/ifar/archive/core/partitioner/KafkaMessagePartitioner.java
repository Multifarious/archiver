package io.ifar.archive.core.partitioner;

public interface KafkaMessagePartitioner {
    ArchivePartitionData archivePartitionFor(String topic, int partition, byte[] rawMessagePayload);
}
