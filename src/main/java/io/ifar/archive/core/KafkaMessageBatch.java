package io.ifar.archive.core;

/** A batch of data read from a single Kafka partition and intended to be written to one or more archive partitions */
public interface KafkaMessageBatch {
    void addMessageToArchiveQueue(String archiveBatchKey, String message) throws Exception;
    void writeToArchive() throws Exception;
    void deleteArchiveBatch() throws InterruptedException;
    int size();
}
