package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.destination;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * Various JMS Connection Factories can be set up in this class. Currently it set up with GLOSS/MQ connection factory by
 * using the connection configuration in jms-destinations.xml
 *
 * @author D540601
 *
 */
public class JmsDestination implements IDestination {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JmsDestination.class);

    private Connection connection;
    private Session session;
    private Queue queue;
    private MessageProducer producer;

    /**
     * Constructor.
     *
     * @throws JMSException
     *             when an error connecting to a queue occurs.
     */
    public JmsDestination() throws JMSException {

        ConnectionFactory connectionFactory = getConnectionFactory();
        connection = connectionFactory.createConnection();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String queueName = null; //JMSConfiguration.getQName();
        queue = session.createQueue(queueName);

        producer = session.createProducer(queue);

    }

    /**
     * Sends the message parameter as a text message into the queue.
     *
     * @param message
     *            the string to send as a message to the queue
     * @throws IllegalArgumentException
     *             if message is blank
     */
    public void sendTextMessage(String message) throws JMSException {
        Validate.notBlank(message, "msg cannot be null");
        Message msg = session.createTextMessage(message);
        producer.send(msg);

        LOGGER.debug("Sent Message to Gloss: " + message);
    }

    /**
     * Get Connection Factory for the jms destinations configured in the jms-destinations.xml
     *
     * @return a ConnectionFactory
     * @throws JMSException
     *             when an error related to create the connection occurs.
     */
    private ConnectionFactory getConnectionFactory() throws JMSException {
       /* MQQueueConnectionFactory mqcf = new MQQueueConnectionFactory();

        mqcf.setHostName(JMSConfiguration.getHostName());
        mqcf.setPort(Integer.parseInt(JMSConfiguration.getPort()));
        mqcf.setQueueManager(JMSConfiguration.getQManager());

        String channel = JMSConfiguration.getChannel();
        if (channel != null) {
            mqcf.setChannel(channel);
        }
        mqcf.setSSLCipherSuite(JMSConfiguration.getSslCipherSuite());

        mqcf.setTransportType(Integer.parseInt(JMSConfiguration.getTransportType()));

        return mqcf;*/
        return null;
    }

    /**
     * Closes the resources used when connecting to the queue.
     */
    public void close() throws JMSException {
        producer.close();
        session.close();
        connection.close();
    }
}
