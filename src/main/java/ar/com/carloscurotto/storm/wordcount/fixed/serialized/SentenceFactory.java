package ar.com.carloscurotto.storm.wordcount.fixed.serialized;

import java.util.Arrays;

import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Sentence;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Word;

public final class SentenceFactory {

    public final Sentence[] createSentences() {
        Sentence[] sentences =
                new Sentence[] {
                        new Sentence(Arrays.asList(new Word[] { new Word("carlos"), new Word("is"), new Word("an"),
                                new Word("engineer") })),
                        new Sentence(Arrays.asList(new Word[] { new Word("victoria"), new Word("is"), new Word("an"),
                                new Word("artist") })) };
        return sentences;
    }
}
