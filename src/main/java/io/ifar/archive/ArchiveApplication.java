package io.ifar.archive;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.mama.Cluster;
import com.fasterxml.mama.ClusterConfig;
import io.ifar.archive.cli.AddPartitionCommand;
import io.ifar.archive.cli.ChangePartitionOffsetCommand;
import io.ifar.archive.cli.DeletePartitionCommand;
import io.ifar.archive.core.ArchiveCluster;
import io.ifar.archive.core.ArchiveListener;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.ifar.archive.core.partitioner.KafkaMessagePartitioner;
import io.ifar.archive.core.partitioner.PerTopicDateRegexKafkaMessagePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class ArchiveApplication extends Application<ArchiveConfiguration> {
    static final Logger LOG = LoggerFactory.getLogger(ArchiveApplication.class);

    public static ObjectMapper MAPPER;

    public static void main(String[] args) throws Exception {
        new ArchiveApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<ArchiveConfiguration> bootstrap) {
        bootstrap.addCommand(new AddPartitionCommand());
        bootstrap.addCommand(new DeletePartitionCommand());
        bootstrap.addCommand(new ChangePartitionOffsetCommand());
    }

    @Override
    public void run(ArchiveConfiguration configuration, Environment environment) throws Exception {
        MAPPER = environment.getObjectMapper();

        S3Configuration s3ContentConfiguration = configuration.getS3Configuration();
        AWSCredentialsProvider credentialsProvider;
        if (s3ContentConfiguration.getAccessKeyId() == null) {
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        } else {
            credentialsProvider = new StaticCredentialsProvider(new BasicAWSCredentials(
                    s3ContentConfiguration.getAccessKeyId(),
                    s3ContentConfiguration.getSecretAccessKey()
            ));
        }
        AmazonS3Client s3Client = new AmazonS3Client(credentialsProvider);

        String workUnitPath = Paths.get(configuration.getZkRoot(), "partitions").toString();

        KafkaMessagePartitioner kafkaMessagePartitioner;
        String customPartitionerClass = configuration.getCustomKafkaMessagePartitionerClass();
        if(customPartitionerClass != null) {
            kafkaMessagePartitioner =
                    (KafkaMessagePartitioner) Class.forName(configuration.getCustomKafkaMessagePartitionerClass()).newInstance();
        } else {
            kafkaMessagePartitioner = new PerTopicDateRegexKafkaMessagePartitioner(
                    configuration.getKafkaMessagePartitionerConfig());
        }

        ArchiveListener listener = new ArchiveListener(s3Client, configuration.getS3Configuration(),
                configuration.getSeedBrokers(), workUnitPath, kafkaMessagePartitioner, configuration.getMaxNumParallelWorkers());
        environment.lifecycle().manage(listener);

        ClusterConfig clusterConfig = new ClusterConfig()
                .hosts(configuration.getZookeeperServerstring())
                .useSmartBalancing(true)
                .autoRebalanceInterval(60 * 5)
                .drainTime(1)
                .workUnitName(workUnitPath)
                .workUnitShortName("partition");
        Cluster ordasityCluster = new Cluster(configuration.getServiceName(), listener, clusterConfig,
                                              environment.metrics());

        ArchiveCluster cluster = new ArchiveCluster(ordasityCluster);
        environment.lifecycle().manage(cluster);

        environment.jersey().disable();
    }
}
