package ar.com.carloscurotto.storm.kafka;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
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

        WordCountsRepository repository = new WordCountsRepository();
        repository.start();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", createKafkaSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(repository), 1).fieldsGrouping("split", new Fields("word"));

        Config configuration = new Config();
        configuration.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("word-count", configuration, builder.createTopology());

        System.out.println("Press any key to stop processing...");
        System.in.read();

        cluster.killTopology("word-count");

        cluster.shutdown();

        System.out.println(repository);

        repository.stop();
    }

}
