package ar.com.carloscurotto.storm.wordcount.kafka;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This is the main class of the famous word count example. We will count words on a fixed quantity of sentences.
 *
 * @author carloscurotto
 */
public class WordCountExample {

    private static IRichSpout createKafkaSpout() {
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        String topic = "test";
        String zkRoot = "/kafkastorm";
        String consumerGroupId = "StormSpout";
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaSentenceScheme());
        return new KafkaSpout(spoutConfig);
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", createKafkaSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));
        StormTopology topology = builder.createTopology();

        Config configuration = new Config();
        configuration.setDebug(true);

        if (args != null && args.length > 0) {
            configuration.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar("word-count", configuration, topology);
        } else {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("word-count", configuration, builder.createTopology());

            System.out.println("Press any key to stop processing...");
            System.in.read();

            cluster.killTopology("word-count");

            cluster.shutdown();
        }

    }

}
