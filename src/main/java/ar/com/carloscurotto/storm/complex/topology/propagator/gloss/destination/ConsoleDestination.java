package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.destination;

import javax.jms.JMSException;

/**
 * A simple destination that writes to the output console. 
 * @author D540601
 */
public class ConsoleDestination implements IDestination {

    /**
     * See {@link IDestination#sendTextMessage(String).
     */
    @Override
    public void sendTextMessage(String theMessage) throws JMSException {
        System.out.println(theMessage + "\n");
    }

    @Override
    public void close() throws JMSException {
        // This class doesn't need to implement something here
    }

}
