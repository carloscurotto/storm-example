package ar.com.carloscurotto.storm.complex.transport;

import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.Openable;

/**
 * Represents an abstraction that will handle the consumption of messages from the transport layer.
 *
 * @author O605461
 *
 * @param <T> the data type of the data to be consumed.
 */
public interface Consumer<T> extends Openable, Closeable {

    /**
     * Receives data in a non-blocking way. This call will return right away returning the data received if there was
     * data in the transport layer ready to be received or null if there was not.
     *
     * @return the data received or null if there was not data ready to be received.
     */
    public T receive();
}
