package ar.com.carloscurotto.storm.wordcount.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
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

        TridentTopology trident = new TridentTopology();
        trident.newStream("spout", createKafkaSpout()).parallelismHint(1).shuffle()
                .each(new Fields("sentence"), new SplitSentenceFunction(), new Fields("word")).parallelismHint(1)
                .groupBy(new Fields("word"))
                .each(new Fields("word"), new WordCountFunction(repository), new Fields("count"));
        StormTopology topology = trident.build();

        Config configuration = new Config();
        configuration.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("word-count", configuration, topology);

        System.out.println("Press any key to stop processing...");
        System.in.read();

        cluster.killTopology("word-count");

        cluster.shutdown();

        System.out.println(repository);

        repository.stop();
    }

}
