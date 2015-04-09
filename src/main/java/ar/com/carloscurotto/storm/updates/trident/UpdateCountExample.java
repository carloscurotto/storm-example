package ar.com.carloscurotto.storm.updates.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.updates.trident.repository.GlossUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.trident.repository.HBaseUpdateCountsRepository;
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

        GlossUpdateCountsRepository glossRepository = new GlossUpdateCountsRepository();

        HBaseUpdateCountsRepository hbaseRepository = new HBaseUpdateCountsRepository();

        TridentTopology trident = new TridentTopology();
        Stream updatesStream =
                trident.newStream("kafka-updates-spout", createTransactionalKafkaSpout()).name("kafka-updates-spout").
                        parallelismHint(1).shuffle();

        Stream externalInternalStream =
                updatesStream.each(new Fields("update"), new ExternalInternalFilter())
                        .each(new Fields("update"), new GlossFunction(glossRepository), new Fields("update-gloss"))
                        .parallelismHint(2).partitionBy(new Fields("update-gloss"));
        externalInternalStream.each(new Fields("update-gloss"), new HBaseFunction(hbaseRepository),
                new Fields("update-hbase")).parallelismHint(2);
        externalInternalStream.name("external-internal");

        Stream internalOnlyStream =
                updatesStream.each(new Fields("update"), new InternalOnlyFilter())
                        .each(new Fields("update"), new HBaseFunction(hbaseRepository), new Fields("update-hbase"))
                        .parallelismHint(2);
        internalOnlyStream.name("internal-only");

        StormTopology topology = trident.build();

        Config configuration = new Config();
        configuration.setDebug(false);

        if (args != null && args.length > 0) {
            configuration.setNumWorkers(5);
            StormSubmitter.submitTopologyWithProgressBar("update-count", configuration, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("update-count", configuration, topology);

            System.out.println("Press any key to stop processing...");
            System.in.read();

            System.out.println(glossRepository);
            System.out.println(hbaseRepository);

            cluster.killTopology("update-count");

            cluster.shutdown();
        }

    }
}