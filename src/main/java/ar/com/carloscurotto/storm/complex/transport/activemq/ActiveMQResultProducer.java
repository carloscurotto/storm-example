package ar.com.carloscurotto.storm.complex.transport.activemq;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.Producer;

public class ActiveMQResultProducer extends OpenAwareBean<Result, Void> implements Producer<Result>, Serializable {

    private static final long serialVersionUID = 1L;

    private String brokerUrl;
    private Connection connection;
    private Session session;
    private Destination replyTopic;
    private MessageProducer producer;

    public ActiveMQResultProducer(final String theBrokerUrl) {
        Validate.notBlank(theBrokerUrl, "The broker url can not be blank.");
        brokerUrl = theBrokerUrl;
    }

    @Override
    public void send(Result theResult) {
        execute(theResult);
    }

    @Override
    protected void doOpen() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            replyTopic = session.createTopic("results");
            producer = session.createProducer(replyTopic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (Exception e) {
            throw new RuntimeException("Error creating active mq producer.", e);
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
            replyTopic = null;
            producer = null;
        }
    }

    @Override
    protected Void doExecute(Result theResult) {
        try {
            BytesMessage message = session.createBytesMessage();
            byte[] bytes = SerializationUtils.serialize(theResult);
            message.writeBytes(bytes);
            message.setJMSCorrelationID(theResult.getId());
            producer.send(message);
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error sending result [" + theResult + "].", e);
        }
    }

}
