package io.ifar.archive.core;

import com.fasterxml.mama.Cluster;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ArchiveCluster implements Managed {
    static final Logger LOG = LoggerFactory.getLogger(ArchiveCluster.class);

    private Cluster cluster;

    public ArchiveCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void start() throws Exception {
        cluster.join();
    }

    @Override
    public void stop() throws Exception {
        cluster.shutdown();
    }

    public void stopAndWait(long waitTime, AtomicBoolean stopFlag) {
        cluster.stopAndWait(waitTime, stopFlag);
    }

}
