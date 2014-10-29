package io.ifar.archive.cli;

import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.core.NodeData;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class AddPartitionCommand extends BaseZkCommand {
    public AddPartitionCommand() {
        super("add-partition", "Adds an Ordasity work unit for the specified Kafka partition to ZooKeeper");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--topic")
                .dest("topic")
                .required(true)
                .help("topic to archive");
        subparser.addArgument("--partition")
                .dest("partition")
                .required(true)
                .help("ID of partition to archive, or comma-separated list of multiple IDs");
        subparser.addArgument("--offset")
                .dest("offset")
                .required(false)
                .type(Integer.class)
                .help("offset at which to start archiving (default: earliest/smallest from Kafka metadata)");
    }

    @Override
    protected void execZkCommand(ZooKeeper zooKeeper, String workUnitBasePath, String topic, String partitionStr, Integer offset) throws Exception {
        Path basePath = Paths.get("/");
        for(Path pathPart : Paths.get(workUnitBasePath)) {
            basePath = basePath.resolve(pathPart);
            if(zooKeeper.exists(basePath.toString(), false) == null) {
                zooKeeper.create(basePath.toString(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }

        ArrayList<String> parts = new ArrayList<>();
        if(partitionStr.contains(",")) {
            for(String part : partitionStr.split(",")) {
                parts.add(part);
            }
        } else {
            parts.add(partitionStr);
        }

        for(String part : parts) {
            zooKeeper.create(workUnitBasePath + topic + "-" + part,
                    ArchiveApplication.MAPPER.writeValueAsBytes(new NodeData(topic, part,
                            offset != null ? offset.toString() : null)),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
