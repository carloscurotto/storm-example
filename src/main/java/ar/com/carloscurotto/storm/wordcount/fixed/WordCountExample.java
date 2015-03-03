package ar.com.carloscurotto.storm.wordcount.fixed;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This is the main class of the famous word count example. We will count words on a fixed quantity of sentences.
 * 
 * @author carloscurotto
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        
        String[] sentences = new String[] {"carlos is an engineer", "victoria is an art teacher"};
        
        WordCountsRepository repository = new WordCountsRepository();
        repository.start();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FixedSentencesSpout(sentences), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(repository), 1).fieldsGrouping("split", new Fields("word"));
        
        Config configuration = new Config();
        configuration.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("word-count", configuration, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("word-count");
        
        cluster.shutdown();
        
        System.out.println(repository);
        
        repository.stop();
    }
    
}
