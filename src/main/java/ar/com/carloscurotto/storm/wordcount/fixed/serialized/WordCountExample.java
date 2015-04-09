package ar.com.carloscurotto.storm.wordcount.fixed.serialized;

import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Sentence;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Word;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.serializer.SentenceSerializer;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.serializer.WordSerializer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * This is the main class of the famous word count example. We will count words on a fixed quantity of sentences.
 *
 * @author carloscurotto
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        
        TridentTopology trident = new TridentTopology();
        trident.newStream("kafka-spout", new FixedSentencesSpout()).name("kafka-spout").parallelismHint(1).shuffle()
                .each(new Fields("sentence"), new SplitSentenceFunction(), new Fields("word")).name("split-sentence").parallelismHint(1)
                .partitionBy(new Fields("word")).name("partition-by-word")
                .each(new Fields("word"), new WordCountFunction(), new Fields("count")).name("word-count").parallelismHint(2);
        StormTopology topology = trident.build();

        Config configuration = new Config();
        configuration.setDebug(false);
        configuration.registerSerialization(Word.class, WordSerializer.class);
        configuration.registerSerialization(Sentence.class, SentenceSerializer.class);

        if (args != null && args.length > 0) {
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
