package ar.com.carloscurotto.storm.complex.topology.spout.activemq;

import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.transport.activemq.ActiveMQConfiguration;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ActiveMQUpdatesSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private static final int TUPLE_SLEEP_MILLIS = 1000 * 1;
    private SpoutOutputCollector collector;
    private ActiveMQConfiguration activeMQConfiguration;
    private Session session;
    private Destination requestQueue;
    private MessageConsumer consumer;

    public ActiveMQUpdatesSpout(final ActiveMQConfiguration theActiveMQConfiguration) {
        Validate.notNull(theActiveMQConfiguration, "The activeMQ configuration cannot be null");
        activeMQConfiguration = theActiveMQConfiguration;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        try {
            activeMQConfiguration.open();
            session = activeMQConfiguration.getSession();
            requestQueue = session.createQueue("updates");
            consumer = session.createConsumer(requestQueue);
            collector = theCollector;
        } catch (Exception e) {
            close();
            throw new RuntimeException("Error opening active mq updates spout.", e);
        }
    }

    @Override
    public void close() {
        closeConsumer();
        activeMQConfiguration.close();
    }

    private void closeConsumer() {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException jmsException) {
            }
        }
    }

    @Override
    public void nextTuple() {
        System.out.println("nextTuple called");
        try {
            BytesMessage request = (BytesMessage) consumer.receiveNoWait();
            if (request != null) {
                byte deserializedBytes[] = new byte[(int) request.getBodyLength()];
                request.readBytes(deserializedBytes);
                Update update = (Update) SerializationUtils.deserialize(deserializedBytes);
                collector.emit(new Values(update));
            }
            Thread.sleep(TUPLE_SLEEP_MILLIS);
        } catch (Exception e) {
            throw new RuntimeException("Error reading request.", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declare(new Fields("update"));
    }

}
