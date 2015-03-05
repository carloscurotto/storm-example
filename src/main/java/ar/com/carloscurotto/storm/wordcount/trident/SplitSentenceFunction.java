package ar.com.carloscurotto.storm.wordcount.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitSentenceFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        String sentence = theTuple.getString(0);
        System.out.println("Sentence tuple received [" + sentence + "] by thread ["
                + Thread.currentThread().getName() + "]");
        for (String word : sentence.split(" ")) {
            theCollector.emit(new Values(word));
        }
    }
}
