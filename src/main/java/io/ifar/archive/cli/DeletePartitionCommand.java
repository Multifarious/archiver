package io.ifar.archive.cli;

import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DeletePartitionCommand extends BaseZkCommand {
    public DeletePartitionCommand() {
        super("delete-partition", "Removes an Ordasity work unit for the specified Kafka partition");
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
                .help("ID of partition to remove work unit for");
    }

    @Override
    protected void execZkCommand(ZooKeeper zooKeeper, String workUnitBasePath, String topic, String partition, Integer offset) throws Exception {
        Stat stat = new Stat();
        String zkPath = workUnitBasePath + topic + "-" + partition;
        String zkData = new String(zooKeeper.getData(zkPath, false, stat));
        int version = stat.getVersion();
        zooKeeper.delete(zkPath, version);
    }
}
