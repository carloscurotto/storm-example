package ar.com.carloscurotto.storm.complex.transport;

import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.Openable;

/**
 * Represents an abstraction that will handle the sending of messages across the transport layer.
 *
 * @author O605461
 */
public interface Producer<T> extends Openable, Closeable {

    /**
     * Sends the given context in a synchronous way. This call will block the caller until the actual send is made. Note
     * that this call does not care about the result, it will only send the data.
     *
     * @param theContext
     *            the data to send.
     */
    public void send(T theContext);
}
