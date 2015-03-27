package ar.com.carloscurotto.storm.complex.service;

/**
 * This is the abstraction that we will use to decouple the updates module from the logic that submits updates to the
 * transport layer and receives responses from it.
 *
 * @author O605461, W506376
 *
 * @param <I>
 *            The request context data type.
 * @param <O>
 *            The response context data type
 */
public abstract class OpenAwareSubmitter<I, O> extends OpenAwareBean {

    /**
     * Sends the given context in a synchronous way, blocking the caller till the result is returned. This call
     * implements the well known request-reply protocol.
     *
     * @param theContext
     *            the context data to send.
     * @return the result received. It may be null.
     */
    public final O submit(I theContext) {
        validateIsOpened();
        return doSubmit(theContext);
    }

    protected abstract O doSubmit(I theContext);
}
