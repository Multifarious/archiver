package io.ifar.archive.core;

import com.boundary.ordasity.Cluster;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
