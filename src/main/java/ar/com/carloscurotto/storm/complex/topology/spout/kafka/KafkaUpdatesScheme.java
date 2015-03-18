package ar.com.carloscurotto.storm.complex.topology.spout.kafka;

import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import ar.com.carloscurotto.storm.complex.model.Update;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class KafkaUpdatesScheme implements Scheme {

    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> deserialize(byte[] bytes) {
        return new Values((Update) SerializationUtils.deserialize(bytes));
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("update");
    }

}