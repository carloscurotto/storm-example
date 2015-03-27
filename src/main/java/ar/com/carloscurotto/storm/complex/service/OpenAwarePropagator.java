package ar.com.carloscurotto.storm.complex.service;

import java.io.Serializable;

/**
 * Represents an abstraction that will propagate the process of a given context and returns its output.
 *
 * @author O605461, W506376
 */
public abstract class OpenAwarePropagator<I, O> extends OpenAwareBean implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Propagates the given context and returns the result.
     *
     * @param theContext
     *            the context object that will be propagated.
     * @return the result of processing the given context. It may be null.
     */
    public final O propagate(I theContext) {
        validateIsOpened();
        return doPropagate(theContext);
    }

    protected abstract O doPropagate(I theContext);

}
