package ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Sentence {

    public Collection<Word> words = new ArrayList<Word>();

    public Sentence(final Collection<Word> theWords) {
        Validate.notNull(theWords, "The words can not be null.");
        words.addAll(theWords);
    }

    public Collection<Word> getWords() {
        return words;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Sentence) {
            Sentence other = (Sentence) object;
            return Objects.equal(words, other.words);            
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(words);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("words", words).toString();
    }
    
}