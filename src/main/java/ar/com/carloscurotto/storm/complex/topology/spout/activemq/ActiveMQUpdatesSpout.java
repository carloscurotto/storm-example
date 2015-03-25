package ar.com.carloscurotto.storm.complex.topology.spout.activemq;

import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Update;
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
    
    private String brokerUrl;
    private Session session;
    private Connection connection;
    private Destination requestTopic;
    private MessageConsumer consumer;
    
    public ActiveMQUpdatesSpout(final String theBrokerUrl) {
        Validate.notBlank(theBrokerUrl, "The broker url can not be blank.");
        brokerUrl = theBrokerUrl;
    }    

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            requestTopic = session.createTopic("updates");
            consumer = session.createConsumer(requestTopic);
            collector = theCollector;
        } catch (Exception e) {
            throw new RuntimeException("Error opening active mq updates spout.", e);
        }        
    }
    
    @Override
    public void close() {
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }            
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error closing active mq updates spout.", e);
        } finally {
            session = null;
            connection = null;
            requestTopic = null;
            consumer = null;
        }                
    }

    @Override
    public void nextTuple() {
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
