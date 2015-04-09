package ar.com.carloscurotto.storm.wordcount.fixed.serialized;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Sentence;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Word;
import backtype.storm.tuple.Values;

public class SplitSentenceFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Sentence sentence = (Sentence)theTuple.getValue(0);
        System.out.println("Sentence tuple received [" + sentence + "] by thread ["
                + Thread.currentThread().getName() + "]");
        for (Word word : sentence.getWords()) {
            theCollector.emit(new Values(word));
        }
    }
}
