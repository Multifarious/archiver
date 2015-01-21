package io.ifar.archive.core;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.s3.AmazonS3Client;
import com.boundary.ordasity.SmartListener;
import io.dropwizard.lifecycle.Managed;
import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.S3Configuration;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.yammer.metrics.scala.Meter;
import io.ifar.archive.core.partitioner.KafkaMessagePartitioner;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveListener extends SmartListener
        implements Managed
{
    final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ScheduledExecutorService executor;
    private final AtomicBoolean stopFlag = new AtomicBoolean(false);

    private final AmazonS3Client s3Client;
    private final S3Configuration s3Configuration;
    private final KafkaMessagePartitioner kafkaMessagePartitioner;

    private final List<String> seedBrokers;
    private final String zkWorkPath;

    private ZooKeeperClient zkClient;

    private final Map<String, WorkerState> workers = new HashMap<String, WorkerState>();

    public ArchiveListener(AmazonS3Client s3Client, S3Configuration s3Configuration, String seedBrokers, String workUnitPath,
                           KafkaMessagePartitioner kafkaMessagePartitioner, int maxNumParallelWorkers) {
        this.s3Client = s3Client;
        this.s3Configuration = s3Configuration;
        this.seedBrokers = Arrays.asList(seedBrokers.split(","));
        this.zkWorkPath = "/" + workUnitPath + "/";
        this.kafkaMessagePartitioner = kafkaMessagePartitioner;
        this.executor = Executors.newScheduledThreadPool(maxNumParallelWorkers);
    }

    @Override
    public void start() throws Exception { }

    @Override
    public void stop() throws Exception {
        stopFlag.set(true);
        List<WorkerState> states;
        synchronized (workers) {
            states = new ArrayList<>(workers.values());
            workers.clear();
        }
        for (WorkerState ws : states) {
            ws.requestStop();
        }
        executor.shutdownNow();
    }

    @Override
    public void startWork(String workUnit, Meter meter) {
        if (stopFlag.get()) {
            LOG.warn("startWork({}) called during shutdown", workUnit);
            return;
        }
        Stat stat = new Stat();
        try {
            byte[] zkData = zkClient.get().getData(zkWorkPath + workUnit, false, stat);
            NodeData data = ArchiveApplication.MAPPER.readValue(zkData, NodeData.class);
            int version = stat.getVersion();

            ArchiveWorker worker = new ArchiveWorker(workUnit, meter, data, version,
                    seedBrokers, s3Client, s3Configuration, zkClient, zkWorkPath, kafkaMessagePartitioner);
            //Future workerFuture = executor.submit(new NamedThreadRunnable(worker, "worker_" + workUnit));
            Future workerFuture = executor.scheduleAtFixedRate(
                    new NamedThreadRunnable(worker.getArchiveBatchTask(), "worker_" + workUnit),
                    0, 1, TimeUnit.MINUTES);
            synchronized (workers) {
                workers.put(workUnit, new WorkerState(worker, workerFuture));
            }
        } catch (Exception e) {
            LOG.warn("startWork() failed", e);
        }
    }


    @Override
    public void onJoin(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void onLeave() { }

    @Override
    public void shutdownWork(String workUnit) {
        WorkerState state;
        synchronized (workers) {
            state = workers.remove(workUnit);
        }
        if (state == null) {
            // Ordasity might feel compelled to call that; if so, consider that ok
            if (!stopFlag.get()) {
                LOG.error("No worker state found for id '{}', ignoring", workUnit);
            }
            return;
        }
        state.requestStop();

        // let's not wait indefinitely for shutdown, 15 seconds better suffice
        final long end = System.currentTimeMillis() + (15 * 1000L);

        while (!state.isTerminated()) {
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

    /**
     * Helper class for encapsulating details of archive worker state handling.
     */
    private static class WorkerState {
        private final ArchiveWorker worker;
        private final Future<?> future;

        public WorkerState(ArchiveWorker worker, Future<?> future) {
            this.worker = worker;
            this.future = future;
        }

        public void requestStop() {
            worker.requestStop();
            future.cancel(true);
        }

        public boolean isTerminated() {
            return future.isDone();
        }
    }
}
