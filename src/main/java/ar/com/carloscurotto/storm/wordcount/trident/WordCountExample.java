package ar.com.carloscurotto.storm.wordcount.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

/**
 * This is the main class of the famous word count example. We will count words on a fixed quantity of sentences.
 *
 * @author carloscurotto
 */
public class WordCountExample {

    private static TransactionalTridentKafkaSpout createTransactionalKafkaSpout() {
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(hosts, "test");
        spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaSentenceScheme());
        return new TransactionalTridentKafkaSpout(spoutConfig);
    }

    public static void main(String[] args) throws Exception {

        TridentTopology trident = new TridentTopology();
        trident.newStream("spout", createTransactionalKafkaSpout()).parallelismHint(1)
                .each(new Fields("sentence"), new SplitSentenceFunction(), new Fields("word"))
                .groupBy(new Fields("word"))
                .each(new Fields("word"), new WordCountFunction(), new Fields("count"));
        StormTopology topology = trident.build();

        Config configuration = new Config();
        configuration.setDebug(false);

        if (args != null && args.length > 0) {
            configuration.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar("word-count", configuration, topology);
        } else {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("word-count", configuration, topology);

            System.out.println("Press any key to stop processing...");
            System.in.read();

            cluster.killTopology("word-count");

            cluster.shutdown();
        }

    }

}
