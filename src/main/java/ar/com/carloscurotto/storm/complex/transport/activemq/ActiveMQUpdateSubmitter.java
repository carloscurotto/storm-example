package ar.com.carloscurotto.storm.complex.transport.activemq;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.UpdateSubmitter;

public class ActiveMQUpdateSubmitter extends OpenAwareBean<Update, Result> implements UpdateSubmitter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQBrokerRunner.class);

    private String brokerUrl;
    private Connection connection;
    private Session session;
    private Destination requestTopic;
    private Destination replyTopic;
    private MessageProducer producer;
    
    public ActiveMQUpdateSubmitter(final String theBrokerUrl) {
        Validate.notBlank(theBrokerUrl, "The broker url can not be blank.");
        brokerUrl = theBrokerUrl;
    }

    @Override
    protected void doOpen() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            requestTopic = session.createTopic("updates");
            replyTopic = session.createTopic("results");
            producer = session.createProducer(requestTopic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (Exception e) {
            throw new RuntimeException("Error creating active mq update submitter.", e);
        }        
    }

    @Override
    protected void doClose() {
        try {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }            
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            session = null;
            connection = null;
            requestTopic = null;
            replyTopic = null;
            producer = null;
        }        
    }

    @Override
    public Result submit(Update theUpdate) {
        return execute(theUpdate);
    }
    
    @Override
    protected Result doExecute(Update theUpdate) {
        MessageConsumer consumer = null;
        try {
            consumer = session.createConsumer(replyTopic, "JMSCorrelationID='" + theUpdate.getId() + "'");

            BytesMessage request = session.createBytesMessage();
            byte[] serializedBytes = SerializationUtils.serialize(theUpdate);
            request.writeBytes(serializedBytes);
            request.setJMSCorrelationID(theUpdate.getId());
            producer.send(request);
            
            BytesMessage response = (BytesMessage) consumer.receive();
            byte deserializedBytes[] = new byte[(int) response.getBodyLength()];
            response.readBytes(deserializedBytes);
            return (Result) SerializationUtils.deserialize(deserializedBytes);
        } catch (Exception e) {
            throw new RuntimeException("Error submitting update [" + theUpdate + "].", e);
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    LOGGER.error("Error closing consumer when submitting update [" + theUpdate + "].", e);
                }
            }
        }
    }

}
