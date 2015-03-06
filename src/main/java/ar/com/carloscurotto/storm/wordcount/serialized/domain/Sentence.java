package ar.com.carloscurotto.storm.wordcount.serialized.domain;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.Validate;

public class Sentence {

    public Collection<Word> words = new ArrayList<Word>();

    public Sentence(final Collection<Word> theWords) {
        Validate.notNull(theWords, "The words can not be null.");
        words.addAll(theWords);
    }

    public Collection<Word> getWords() {
        return words;
    }
}