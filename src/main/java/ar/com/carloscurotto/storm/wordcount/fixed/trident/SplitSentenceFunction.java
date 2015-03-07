package ar.com.carloscurotto.storm.wordcount.fixed.trident;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class SplitSentenceFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        String sentence = (String)theTuple.getValue(0);
        System.out.println("Sentence tuple received [" + sentence + "] by thread ["
                + Thread.currentThread().getName() + "]");
        for (String word : sentence.split(" ")) {
            theCollector.emit(new Values(word));
        }
    }
}
