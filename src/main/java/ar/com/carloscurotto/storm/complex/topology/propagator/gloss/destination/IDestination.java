package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.destination;

import javax.jms.JMSException;

/**
 * Interface to access any kind of Destination implementation.
 *
 * @author D540601
 *
 */
public interface IDestination {

    /**
     * Sends a text message into the queue destination
     *
     * @param theMessage
     *            a String to be sent into the destination.
     * @throws Exception
     *             when sending the message fails.
     */
    public void sendTextMessage(String theMessage) throws JMSException;

    /**
     * Closes any connection or queues this destination may hold.
     *
     * @throws JMSException
     *             when an error occurs releasing resources related to jms queues.
     */
    public void close() throws JMSException;
}
