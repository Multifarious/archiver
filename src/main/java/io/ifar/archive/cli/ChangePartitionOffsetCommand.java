package io.ifar.archive.cli;

import io.ifar.archive.ArchiveApplication;
import io.ifar.archive.core.NodeData;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ChangePartitionOffsetCommand extends BaseZkCommand {
    public ChangePartitionOffsetCommand() {
        super("change-partition-offset", "Alters the offset from which archiving will continue for the specified partition");
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
                .help("ID of partition to archive");
        subparser.addArgument("--offset")
                .dest("offset")
                .required(true)
                .type(Integer.class)
                .help("offset at which to (re)start archiving");
    }

    @Override
    protected void execZkCommand(ZooKeeper zooKeeper, String workUnitBasePath, String topic, String partition, Integer offset) throws Exception {
        Stat stat = new Stat();
        String zkPath = workUnitBasePath + topic + "-" + partition;
        String zkData = new String(zooKeeper.getData(zkPath, false, stat));
        int version = stat.getVersion();
        zooKeeper.setData(zkPath,
                ArchiveApplication.MAPPER.writeValueAsBytes(new NodeData(topic, partition, offset.toString())),
                version);
    }
}
