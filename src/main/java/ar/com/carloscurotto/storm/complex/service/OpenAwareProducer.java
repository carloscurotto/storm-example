package ar.com.carloscurotto.storm.complex.service;

/**
 * Represents an abstraction that will handle the sending of messages across the transport layer.
 *
 * @author O605461, W506376
 */
public abstract class OpenAwareProducer<T> extends OpenAwareBean {

    /**
     * Sends the given context in a synchronous way. This call will block the caller until the actual send is made. Note
     * that this call does not care about the result, it will only send the data.
     *
     * @param theContext
     *            the data to send. It can be null.
     */
    public final void send(T theContext) {
        validateIsOpened();
        doSend(theContext);
    }

    protected abstract void doSend(T theContext);
}
