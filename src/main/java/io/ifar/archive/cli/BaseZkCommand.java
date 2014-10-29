package io.ifar.archive.cli;

import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.ArchiveConfiguration;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;

abstract public class BaseZkCommand extends ConfiguredCommand<ArchiveConfiguration> {
    protected BaseZkCommand(String name, String description) {
        super(name, description);
    }

    @Override
    protected void run(Bootstrap<ArchiveConfiguration> archiveConfigurationBootstrap, Namespace namespace, ArchiveConfiguration configuration) throws Exception {
        ArchiveApplication.MAPPER = archiveConfigurationBootstrap.getObjectMapper();

        String topic = namespace.getString("topic");
        String partition = namespace.getString("partition");
        Integer offset = namespace.getInt("offset");

        String[] servers = configuration.getZookeeperServerstring().split(",");
        ArrayList<InetSocketAddress> zkServers = new ArrayList<>();
        for(String server : servers) {
            String host = server.split(":")[0];
            int port = Integer.parseInt(server.split(":")[1]);
            zkServers.add(new InetSocketAddress(host, port));
        }
        ZooKeeperClient client = new ZooKeeperClient(Amount.of(10000, Time.MILLISECONDS), zkServers);
        String workUnitPath = Paths.get(configuration.getZkRoot(), "partitions").toString();
        try {
            execZkCommand(client.get(), "/" + workUnitPath + "/", topic, partition, offset);
        } finally {
            client.close();
        }
    }

    protected abstract void execZkCommand(ZooKeeper zooKeeper, String workUnitBasePath, String topic, String partition, Integer offset) throws Exception;
}
