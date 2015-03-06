package ar.com.carloscurotto.storm.wordcount.serialized.domain;

import org.apache.commons.lang3.Validate;

public class Word {

    private String value;

    public Word(final String theValue) {
        Validate.notBlank(theValue, "The value can not be blank.");
        value = theValue;
    }

    public String getValue() {
        return value;
    }
}
