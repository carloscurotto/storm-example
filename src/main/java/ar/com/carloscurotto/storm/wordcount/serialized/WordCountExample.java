package ar.com.carloscurotto.storm.wordcount.serialized;

import java.util.Arrays;

import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.wordcount.serialized.domain.Sentence;
import ar.com.carloscurotto.storm.wordcount.serialized.domain.Word;
import ar.com.carloscurotto.storm.wordcount.serialized.serializer.SentenceSerializer;
import ar.com.carloscurotto.storm.wordcount.serialized.serializer.WordSerializer;
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

        Sentence[] sentences =
                new Sentence[] {
                        new Sentence(Arrays.asList(new Word[] { new Word("carlos"), new Word("is"), new Word("an"),
                                new Word("engineer") })),
                        new Sentence(Arrays.asList(new Word[] { new Word("victoria"), new Word("is"), new Word("an"),
                                new Word("artist") })) };

        TridentTopology trident = new TridentTopology();
        trident.newStream("kafka-spout", new FixedSentencesSpout(sentences)).name("kafka-spout").parallelismHint(1).shuffle()
                .each(new Fields("sentence"), new SplitSentenceFunction(), new Fields("word")).name("split-sentence").parallelismHint(3)
                .partitionBy(new Fields("word")).name("group-by-word")
                .each(new Fields("word"), new WordCountFunction(), new Fields("count")).toStream().name("word-count").parallelismHint(5);
        StormTopology topology = trident.build();

        System.out.println(topology.toString());

        Config configuration = new Config();
        configuration.setDebug(true);
        configuration.setNumWorkers(4);
        configuration.registerSerialization(Word.class, WordSerializer.class);
        configuration.registerSerialization(Sentence.class, SentenceSerializer.class);

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
