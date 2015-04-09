package ar.com.carloscurotto.storm.wordcount.kafka;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class KafkaSentenceScheme implements Scheme {

    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String update = new String(bytes, "UTF-8");
            return new Values(update);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("sentence");
    }

}
