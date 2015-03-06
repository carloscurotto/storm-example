package ar.com.carloscurotto.storm.wordcount.serialized;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class WordCountFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private WordCountsRepository counts;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map configuration, TridentOperationContext context) {
        System.out.println("Initializing counts...");
        counts = new WordCountsRepository();
        counts.start();
        System.out.println("Initialized counts.");
    }

    @Override
    public void cleanup() {
        System.out.println("Destroying counts...");
        System.out.println(counts);
        counts.stop();
        counts = null;
        System.out.println("Destroyed counts.");
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        counts.put(word, count);
        System.out.println("Word tuple received [" + word + ", " + count + "] by thread ["
                + Thread.currentThread().getName() + "]");
    }
}
