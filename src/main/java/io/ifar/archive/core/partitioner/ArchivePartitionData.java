package io.ifar.archive.core.partitioner;

import java.util.Date;

public class ArchivePartitionData {
    public final byte[] message;
    public final String archivePartition;
    public final Date archiveTime;

    public ArchivePartitionData(byte[] message, String archivePartition, Date archiveTime) {
        this.message = message;
        this.archivePartition = archivePartition;
        this.archiveTime = archiveTime;
    }
}
