package ar.com.carloscurotto.storm.complex.transport;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.Openable;

/**
 * This is the abstraction that we will use to decouple the updates module from the logic that submits updates to the
 * transport layer and receives responses from it. We call it submitter since its responsibility is to submit requests
 * and receive responses.
 *
 * @author O605461
 * @param <I>
 *            The request context data type.
 * @param <O>
 *            The response context data type
 */
public interface Submitter extends Openable, Closeable {

    /**
     * Sends the given context in a synchronous way, blocking the caller till the result is returned. This call
     * implements the well known request-reply protocol.
     *
     * @param theContext
     *            the context data to send.
     * @return the result received.
     */
    public Result submit(Update theContext);
}
