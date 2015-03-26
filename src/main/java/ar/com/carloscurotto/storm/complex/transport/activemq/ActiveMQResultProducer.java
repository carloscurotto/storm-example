package ar.com.carloscurotto.storm.complex.transport.activemq;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;

public class ActiveMQResultProducer extends OpenAwareProducer<Result> implements Serializable {

    private static final long serialVersionUID = 1L;

    private ActiveMQConfiguration activeMQConfiguration;
    private Session session;
    private Destination replyTopic;
    private MessageProducer producer;

    public ActiveMQResultProducer(final ActiveMQConfiguration theActiveMQConfiguration) {
        Validate.notNull(theActiveMQConfiguration, "The activeMQ configuration cannot be null");
        activeMQConfiguration = theActiveMQConfiguration;
    }

    @Override
    protected void doOpen() {
        try {
            activeMQConfiguration.open();
            session = activeMQConfiguration.getSession();
            replyTopic = session.createTopic("results");
            producer = session.createProducer(replyTopic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (Exception e) {
            activeMQConfiguration.close();
            doClose();
            throw new RuntimeException("Error creating active mq producer.", e);
        }
    }

    @Override
    public void doSend(Result theResult) {
        try {
            BytesMessage message = session.createBytesMessage();
            byte[] bytes = SerializationUtils.serialize(theResult);
            message.writeBytes(bytes);
            message.setJMSCorrelationID(theResult.getId());
            producer.send(message);
        } catch (Exception e) {
            throw new RuntimeException("Error sending result [" + theResult + "].", e);
        }
    }

    @Override
    protected void doClose() {
        closeProducer();
    }

    private void closeProducer() {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException jmsException) {
            }
        }
    }
}
