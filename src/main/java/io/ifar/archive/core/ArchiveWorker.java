package io.ifar.archive.core;

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Lists;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.yammer.metrics.scala.Meter;
import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.S3Configuration;
import io.ifar.archive.core.partitioner.ArchivePartitionData;
import io.ifar.archive.core.partitioner.KafkaMessagePartitioner;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

class ArchiveWorker implements Runnable {
    final static Logger LOG = LoggerFactory.getLogger(ArchiveWorker.class);

    private final static int maxBatchTime = 60000;
    private final static int maxBatchCount = 10000;
    private final static int fetchSize = 1000000;
    private final static int simpleConsumerTimeout = 10000;
    private final static int simpleConsumerBufferSize = 1024000;

    private final AmazonS3Client s3Client;
    private final S3Configuration s3Configuration;
    private final KafkaMessagePartitioner kafkaMessagePartitioner;

    private final AtomicBoolean stopFlag = new AtomicBoolean(false);

    private final ZooKeeperClient zkClient;
    private final String zkWorkPath;

    private final List<String> seedBrokers;
    private final List<String> replicaBrokers;

    private final String workUnit;
    public final Meter meter;
    private NodeData partitionState;
    private int version;

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public ArchiveWorker(String workUnit, Meter meter, NodeData data, int version,
                         List<String> seedBrokers,
                         AmazonS3Client s3Client, S3Configuration s3Configuration,
                         ZooKeeperClient zkClient,
                         String zkWorkPath,
                         KafkaMessagePartitioner kafkaMessagePartitioner)
    {
        this.seedBrokers = seedBrokers;
        replicaBrokers = Lists.newArrayList(seedBrokers);
        this.workUnit = workUnit;
        this.meter = meter;
        this.partitionState = data;
        this.version = version;
        this.s3Client = s3Client;
        this.s3Configuration = s3Configuration;
        this.zkClient = zkClient;
        this.zkWorkPath = zkWorkPath;
        this.kafkaMessagePartitioner = kafkaMessagePartitioner;
    }

    public void requestStop() {
        stopFlag.set(true);
    }

    public boolean isTerminated() {
        return terminated.get();
    }

    private String getNextS3Key(String fileKeyPrefix) throws InterruptedException {
        try {
            ObjectListing objectListing = s3Client.listObjects(s3Configuration.getBucket(), fileKeyPrefix);
            List<S3ObjectSummary> os = objectListing.getObjectSummaries();
            int filenum = 0;
            if(os.size() > 0) {
                String lastKey = os.get(os.size() - 1).getKey();
                String[] keyParts = lastKey.split("_");
                String filenumStr = keyParts[keyParts.length - 1].split("\\.")[0];
                filenum = Integer.parseInt(filenumStr) + 1;
            }
            return fileKeyPrefix + "_" + workUnit + "_" + filenum + ".dat";
        } catch(AbortedException e) {
            LOG.info("AbortedException thrown while listing S3 objects; throwing InterruptedException", e);
            throw new InterruptedException();
        }
    }

    public void commit(Long offset) throws ZooKeeperClient.ZooKeeperConnectionException, IOException, KeeperException, InterruptedException {
        partitionState.setOffset(offset.toString());

        Stat stat = zkClient.get()
                      .setData(zkWorkPath + workUnit,
                              ArchiveApplication.MAPPER.writeValueAsBytes(partitionState),
                              version);
        version = stat.getVersion();
    }

    private PartitionMetadata findLeader() {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                String[] hostAndPort = seed.split(":");
                String seedHost = hostAndPort[0];
                int seedPort = Integer.parseInt(hostAndPort[1]);

                consumer = new SimpleConsumer(seedHost, seedPort, simpleConsumerTimeout, 64 * 1024, "leaderLookup");

                List<String> topics = Collections.singletonList(partitionState.getTopic());
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partitionState.partition()) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error communicating with broker [{}] to find leader for [{}, {}]: {}",
                        seed, partitionState.getTopic(), partitionState.getPartition(), e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }

        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host() + ":" + replica.port());
            }
        }

        return returnMetaData;
    }

    private Broker findNewLeader(Broker oldLeader) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.host().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        LOG.error("Unable to find new leader after broker failure. Exiting");
        throw new Exception("Unable to find new leader after broker failure. Exiting");
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOG.error("Error fetching partitionState offset partitionState from broker: {}", response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }


    @Override
    public void run() {
        // This loop forms what used to be considered "supervisor"

        int count = 0;

        try {
            while (!stopFlag.get()) {
                if (++count > 1) {
                    LOG.info("Restarting archive worker '{}'", workUnit);
                }
                try {
                    runLoop();
                } catch (Throwable t) {
                    // let's try to detect "valid" exit via InterruptedException
                    while (t.getCause() != null) {
                        t = t.getCause();
                    }
                    if ((t instanceof InterruptedException) && stopFlag.get()) {
                        break;
                    }
                    LOG.warn("Archive worker loop interrupted due to uncaught exception: " + t.getMessage(), t);
                }
                if (stopFlag.get()) {
                    break;
                }
                // Could make thread wait exponential amount of time, but does not seem necessary here
                try {
                    Thread.sleep(1000L);
                } catch (Throwable t) {
                }
            }
        } finally {
            terminated.set(true);
        }
    }

    private void runLoop() throws InterruptedException
    {
        SimpleConsumer consumer = null;
        KafkaMessageBatch messageBatch = null;

        try {
            LOG.info("Archive worker thread started for {}, partition {}, starting offset of {}",
                    partitionState.getTopic(), partitionState.getPartition(), partitionState.getOffset());

            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                // Metadata will not be present if nothing is in the partition yet. Retry in-thread for a while
                // to minimize extraneous log errors.
                for(int i = 10; i > 0; i--) {
                    LOG.warn("Can't find metadata for topic {} and partition {}. Retrying in 30 seconds. " +
                            "Will retry {} more times.", partitionState.getTopic(), partitionState.getPartition(), i);
                    Thread.sleep(30000);
                    metadata = findLeader();
                    if(metadata != null) break;
                }
                LOG.warn("Can't find metadata for topic {} and partition {}. Exiting", partitionState.getTopic(), partitionState.getPartition());
                return;
            }

            if (metadata.leader() == null) {
                LOG.error("Can't find leader for topic {} and partition {}. Exiting", partitionState.getTopic(), partitionState.getPartition());
                return;
            }

            Broker leadBroker = metadata.leader();
            String clientName = "archive_" + partitionState.getTopic() + "_" + partitionState.partition();

            long readOffset = 0;
            if(partitionState.getOffset() == null) {
                LOG.info("Offset is null for partition {}, getting earliest offset from Kafka", partitionState.getPartition());
                consumer = new SimpleConsumer(leadBroker.host(), leadBroker.port(), simpleConsumerTimeout, simpleConsumerBufferSize, clientName);
                readOffset = getLastOffset(consumer, partitionState.getTopic(), partitionState.partition(), kafka.api.OffsetRequest.EarliestTime(), clientName);
                LOG.info("Found earliest offset of {} for partition {}", readOffset, partitionState.getPartition());
                commit(readOffset);
            }
            else {
                readOffset = Long.parseLong(partitionState.getOffset());
            }

            int numErrors = 0;
            while (true) {
                Map<String, String> messageBatchKeys = new HashMap<>();
                messageBatch = new TempFileKafkaMessageBatch(s3Configuration, s3Client);
                long batchStartOffset = readOffset;
                LOG.debug(String.format("Starting batch for worker %s at offset %d", workUnit, readOffset));
                // we want to write out and commit a batch approximately every minute (or every n records)
                int batchNumRead = 0;
                long batchStart = System.currentTimeMillis();

                while((System.currentTimeMillis() - batchStart) < maxBatchTime && (batchNumRead < maxBatchCount)) {
                    if (endIsNigh()) {
                        messageBatch.deleteArchiveBatch();
                        return;
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("%s:\t%d\t%d", workUnit, (System.currentTimeMillis() - batchStart), batchNumRead));
                    }
                    if (consumer == null) {
                        consumer = new SimpleConsumer(leadBroker.host(), leadBroker.port(), simpleConsumerTimeout, simpleConsumerBufferSize, clientName);
                    }
                    kafka.api.FetchRequest req = new FetchRequestBuilder()
                            .clientId(clientName)
                            .addFetch(partitionState.getTopic(), partitionState.partition(), readOffset, fetchSize)
                            .build();
                    FetchResponse fetchResponse = consumer.fetch(req);

                    if (fetchResponse.hasError()) {
                        numErrors++;
                        // Something went wrong!
                        short code = fetchResponse.errorCode(partitionState.getTopic(), partitionState.partition());
                        LOG.error("Error fetching partitionState from broker {}: {}", leadBroker, code);
                        if (numErrors > 5) throw new RuntimeException("Couldn't fetch partitionState from broker after 5 tries. Exiting.");
                        if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                            // We asked for an invalid offset. For simple case ask for the last element to reset
                            readOffset = getLastOffset(consumer, partitionState.getTopic(), partitionState.partition(), kafka.api.OffsetRequest.LatestTime(), clientName);
                            continue;
                        }
                        consumer.close();
                        consumer = null;
                        try {
                            leadBroker = findNewLeader(leadBroker);
                        } catch (Exception e) {
                            LOG.warn("Problems finding new leader", e);
                        }
                        continue;
                    }
                    numErrors = 0;

                    long numRead = 0;
                    final String topic = partitionState.getTopic();
                    final int partition = partitionState.partition();
                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                        if (endIsNigh()) {
                            messageBatch.deleteArchiveBatch();
                            return;
                        }
                        long currentOffset = messageAndOffset.offset();
                        if (currentOffset < readOffset) {
                            LOG.error("Found an old offset {}, expecting {}", currentOffset, readOffset);
                            continue;
                        }
                        readOffset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();

                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);

                        ArchivePartitionData apd =
                                kafkaMessagePartitioner.archivePartitionFor(topic, partition, bytes);
                        byte[] message = apd.message;
                        String archivePartition = apd.archivePartition;
                        Date dt = apd.archiveTime;

                        String fileKeyPrefix = String.format(
                                "%s/%s/minute/%tY/%tm/%td/%tH/%s_%tY-%tm-%tdT%tH:%tM",
                                topic, archivePartition, dt, dt, dt, dt, topic, dt, dt, dt, dt, dt);
                        String fileKey = messageBatchKeys.get(fileKeyPrefix);
                        if (fileKey == null) {
                            fileKey = getNextS3Key(fileKeyPrefix);
                            messageBatchKeys.put(fileKeyPrefix, fileKey);
                        }

                        messageBatch.addMessageToArchiveQueue(fileKey, message);
                        numRead++;
                    }

                    if (numRead == 0) {
                        Thread.sleep(1000);
                    }
                    else {
                        meter.mark(numRead);
                        batchNumRead += numRead;
                    }
                }

                if (messageBatch.size() > 0) {
                    try {
                        messageBatch.writeToArchive();
                        messageBatch = null; // to ensure no rollback
                        commit(readOffset);
                    } catch (InterruptedException e) {
                        LOG.info(String.format("Worker thread %s interrupted while writing and committing batch; " +
                                "deleting S3 files and terminating worker thread", workUnit));
                        if (messageBatch != null) {
                            messageBatch.deleteArchiveBatch();
                        }
                        return;
                    } catch (KeeperException.BadVersionException e) {
                        LOG.warn("Zookeeper version out of sync for worker {}. Terminating worker thread.", workUnit);
                        if (messageBatch != null) {
                            messageBatch.deleteArchiveBatch();
                        }
                        return;
                    } catch (Exception e) {
                        readOffset = batchStartOffset;
                        try {
                            LOG.warn(String.format("Exception writing batch for worker %s; deleting S3 files and resetting read offset to %d", workUnit, batchStartOffset), e);
                            if (messageBatch != null) {
                                messageBatch.deleteArchiveBatch();
                            }
                        } catch (Exception re) {
                            LOG.error("Couldn't delete S3 files for batch while rolling back.", re);
                            if (re instanceof RuntimeException) {
                                throw (RuntimeException) re;
                            }
                            throw new RuntimeException(re);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Ok; we failed, let's roll back (note: deletion is idempotent)
            if (messageBatch != null) {
                try {
                    messageBatch.deleteArchiveBatch();
                } catch (Exception e2) {
                    LOG.warn("Secondary fail during archive roll back; possibly harmless: {}", e2.getMessage());
                }
            }
            if (e instanceof InterruptedException) { // cleaner if caller can detect this appropriately
                throw (InterruptedException) e;
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            LOG.error("Unhandled exception in ArchiveWorker thread", e);
            throw new RuntimeException(e);
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    LOG.warn("Problems closing SimpleConsumer", e);
                }
            }
        }
    }

    private boolean endIsNigh() {
        if (stopFlag.get()) {
            LOG.info("Archive worker {} stop requested. Terminating worker.", workUnit);
            return true;
        }
        if (Thread.interrupted()) {
            LOG.info("Archive worker {} interrupted. Terminating worker.", workUnit);
            return true;
        }
        return false;
    }
}
