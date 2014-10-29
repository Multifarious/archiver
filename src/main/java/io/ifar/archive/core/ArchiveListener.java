package io.ifar.archive.core;

import com.amazonaws.services.s3.AmazonS3Client;
import com.boundary.ordasity.SmartListener;
import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.S3Configuration;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.yammer.metrics.scala.Meter;
import io.ifar.archive.core.partitioner.KafkaMessagePartitioner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class ArchiveListener extends SmartListener {
    final Logger LOG = LoggerFactory.getLogger(getClass());

    private ExecutorService executor;

    static class NamedThreadRunnable implements Runnable {
        private final Runnable runnable;
        private final String threadName;
        public NamedThreadRunnable(Runnable runnable, String threadName) {
            this.runnable = runnable;
            this.threadName = threadName;
        }
        @Override
        public void run() {
            String originalName = Thread.currentThread().getName();
            Thread.currentThread().setName(threadName);
            try {
                runnable.run();
            } finally {
                Thread.currentThread().setName(originalName);
            }
        }
    }

    private final AmazonS3Client s3Client;
    private final S3Configuration s3Configuration;
    private final KafkaMessagePartitioner kafkaMessagePartitioner;

    private List<String> seedBrokers = new ArrayList<String>();

    private ZooKeeperClient zkClient;
    private String zkWorkPath;

    private ConcurrentHashMap<String, Future> workerFutures = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ArchiveWorker> workers = new ConcurrentHashMap<>();
    private Future<?> archiveSupervisor;

    public ArchiveListener(AmazonS3Client s3Client, S3Configuration s3Configuration, String seedBrokers, String workUnitName,
                           KafkaMessagePartitioner kafkaMessagePartitioner) {
        this.s3Client = s3Client;
        this.s3Configuration = s3Configuration;
        this.seedBrokers = Arrays.asList(seedBrokers.split(","));
        this.zkWorkPath = "/" + workUnitName + "/";
        this.kafkaMessagePartitioner = kafkaMessagePartitioner;
    }

    @Override
    public void startWork(String workUnit, Meter meter) {
        Stat stat = new Stat();
        try {
            byte[] zkData = zkClient.get().getData(zkWorkPath + workUnit, false, stat);
            NodeData data = ArchiveApplication.MAPPER.readValue(zkData, NodeData.class);
            int version = stat.getVersion();

            ArchiveWorker worker = new ArchiveWorker(workUnit, meter, data, version,
                    seedBrokers, s3Client, s3Configuration, zkClient, workers, zkWorkPath, kafkaMessagePartitioner);
            workers.put(workUnit, worker);
            Future workerFuture = executor.submit(new NamedThreadRunnable(worker, "worker_" + workUnit));
            workerFutures.put(workUnit, workerFuture);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) { // includes Json[Xxx]Exceptions
            e.printStackTrace();
        }
        // need to figure out how to deal with exceptions here
    }

    @Override
    public void onJoin(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
        this.executor = Executors.newCachedThreadPool();
        archiveSupervisor = executor.submit(new ArchiveSupervisor());
    }

    @Override
    public void onLeave() {
        executor.shutdownNow();
    }

    @Override
    public void shutdownWork(String workUnit) {
        Future workerFuture = workerFutures.remove(workUnit);
        if(workerFuture != null) {
            workerFuture.cancel(true);
        }
        ArchiveWorker worker = workers.remove(workUnit);
        if (worker == null) {
            LOG.error("No worker found for id '{}', ignoring", workUnit);
            return;
        }

        // let's not wait indefinitely for shutdown, 15 seconds better suffice
        final long end = System.currentTimeMillis() + (15 * 1000L);

        while (!worker.isTerminated()) {
            LOG.info("Waiting one second for worker {} to terminate...", workUnit);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for worker {} to terminate", workUnit);
                Thread.currentThread().interrupt();
            }
            if (System.currentTimeMillis() > end) {
                LOG.warn("Waited for maximum amount of time (15 seconds) for worker {} to shut down; skipping", workUnit);
                break;
            }
        }
    }

    // Polls active workers and restarts them if they unexpectedly die.
    // The only expected way for an ArchiveWorker to end is explicit cancellation via shutdownWork().
    private class ArchiveSupervisor implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    if(Thread.interrupted()) {
                        LOG.info("ArchiveSupervisor interrupted, exiting");
                        return;
                    }

                    for(Map.Entry<String, Future> entry : workerFutures.entrySet()) {
                        if(entry.getValue().isDone()) {
                            String workUnit = entry.getKey();
                            LOG.error(String.format("Unexpected archive worker completion for work unit %s; restarting worker", workUnit));
                            workerFutures.remove(workUnit);
                            ArchiveWorker worker = workers.remove(workUnit);
                            startWork(workUnit, worker.meter);
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch(InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch(Exception e) {
                LOG.error("Unhandled exception in ArchiveSupervisor", e);
                throw e;
            }
        }
    }

}
