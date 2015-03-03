package ar.com.carloscurotto.storm.wordcount.trident;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class WordCountFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private WordCountsRepository counts;

    public WordCountFunction(WordCountsRepository theCounts) {
        counts = theCounts;
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
        System.out.println("Word tuple received [" + word + ", " + count + "]");
    }
}
