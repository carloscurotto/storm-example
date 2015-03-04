package ar.com.carloscurotto.storm.updates.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import ar.com.carloscurotto.storm.updates.trident.repository.GlossUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.trident.repository.HBaseUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.trident.repository.HazelcastInstanceProvider;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

public class UpdateCountExample {

    private static TransactionalTridentKafkaSpout createTransactionalKafkaSpout() {
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(hosts, "test");
        spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaSentenceScheme());
        return new TransactionalTridentKafkaSpout(spoutConfig);
    }

    public static void main(String[] args) throws Exception {

        HazelcastInstanceProvider.start();

        GlossUpdateCountsRepository glossRepository = new GlossUpdateCountsRepository();

        HBaseUpdateCountsRepository hbaseRepository = new HBaseUpdateCountsRepository();

        TridentTopology trident = new TridentTopology();
        Stream updatesStream = trident.newStream("spout", createTransactionalKafkaSpout()).parallelismHint(1);

        GroupedStream externalInternalStream =
                updatesStream.each(new Fields("update"), new ExternalInternalFilter()).parallelismHint(5)
                        .groupBy(new Fields("update"));
        externalInternalStream
                .each(new Fields("update"), new GlossFunction(glossRepository), new Fields("update-gloss")).toStream()
                .groupBy(new Fields("update-gloss"))
                .each(new Fields("update-gloss"), new HBaseFunction(hbaseRepository), new Fields("update-hbase"));

        GroupedStream internalOnlyStream =
                updatesStream.each(new Fields("update"), new InternalOnlyFilter()).parallelismHint(5)
                        .groupBy(new Fields("update"));
        internalOnlyStream.each(new Fields("update"), new HBaseFunction(hbaseRepository), new Fields("update-hbase"));

        StormTopology topology = trident.build();

        Config configuration = new Config();
        configuration.setDebug(false);

        if (args != null && args.length > 0) {
            configuration.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar("update-count", configuration, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("update-count", configuration, topology);

            System.out.println("Press any key to stop processing...");
            System.in.read();

            cluster.killTopology("update-count");

            cluster.shutdown();

            System.out.println(glossRepository);
            System.out.println(hbaseRepository);

            HazelcastInstanceProvider.stop();
        }

    }
}