package io.ifar.archive.core.partitioner;

import java.util.Date;

public interface KafkaMessagePartitioner {
    ArchivePartitionData archivePartitionFor(String topic, int partition, byte[] rawMessagePayload);

    public static class ArchivePartitionData {
        public String message;
        public String archivePartition;
        public Date archiveTime;

        public ArchivePartitionData(String message, String archivePartition, Date archiveTime) {
            this.message = message;
            this.archivePartition = archivePartition;
            this.archiveTime = archiveTime;
        }
    }
}
