package ar.com.carloscurotto.storm.complex.transport.activemq;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.OpenAwareSubmitter;

public class ActiveMQUpdateSubmitter extends OpenAwareSubmitter<Update, Result> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQUpdateSubmitter.class);

    private ActiveMQConfiguration activeMQConfiguration;
    private Session session;
    private Destination requestQueue;
    private Destination replyQueue;
    private MessageProducer producer;

    public ActiveMQUpdateSubmitter(final ActiveMQConfiguration theActiveMQConfiguration) {
        Validate.notNull(theActiveMQConfiguration, "The activeMQ configuration cannot be null");
        activeMQConfiguration = theActiveMQConfiguration;
    }

    @Override
    protected void doOpen() {
        try {
            activeMQConfiguration.open();
            session = activeMQConfiguration.getSession();
            requestQueue = session.createQueue("updates");
            replyQueue = session.createQueue("results");
            producer = session.createProducer(requestQueue);
        } catch (Exception e) {
            close();
            throw new RuntimeException("Error creating active mq update submitter.", e);
        }
    }

    @Override
    protected void doClose() {
        activeMQConfiguration.close();
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

    @Override
    public Result doSubmit(Update theUpdate) {
        MessageConsumer consumer = null;
        try {
            consumer = session.createConsumer(replyQueue, "JMSCorrelationID='" + theUpdate.getId() + "'");

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
