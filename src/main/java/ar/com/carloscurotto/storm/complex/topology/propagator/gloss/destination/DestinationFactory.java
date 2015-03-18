package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.destination;

import javax.jms.JMSException;

/**
 * Creates queue destinations.
 *
 * @author D540601
 *
 */
public class DestinationFactory {

    /**
     * Returns the default jms destination.
     *
     * @return the default jms destination
     * @throws JMSException
     *             when destination cannot be created
     */
    public static IDestination getDefaultDestination() throws JMSException {
        return new JmsDestination();
    }

    /**
     * Returns the console destination, which will write to the system console when sending a message
     *
     * @return a IDestination console destination.
     */
    public static IDestination getConsoleDestination() {
        return new ConsoleDestination();
    }
}