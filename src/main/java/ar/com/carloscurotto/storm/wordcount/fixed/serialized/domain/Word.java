package ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Word {

    private String value;

    public Word(final String theValue) {
        Validate.notBlank(theValue, "The value can not be blank.");
        value = theValue;
    }

    public String getValue() {
        return value;
    }
    
    @Override
    public boolean equals(Object object) {
        if (object instanceof Word) {
            Word other = (Word) object;
            return Objects.equal(value, other.value);            
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("value", value).toString();
    }
}
