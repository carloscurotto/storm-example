package ar.com.carloscurotto.storm.complex.service;

/**
 * This contract represents a component that performs a particular action and
 * returns the result of the execution.
 *
 * @author O605461
 *
 * @param <T>
 *            the execution context's type.
 * @param <R>
 *            the execution result's type.
 */
public interface Service<T, R> extends Openable, Closeable {

    /**
     * Performs the particular action associated to this service. It can receive
     * an object that acts as the execution context and returns the result of
     * the execution. This method is synchronous, this means that we will return
     * the control to the caller after the execution is completed.
     *
     * @param theContext
     *            the execution context.
     * @return the execution result.
     */
    public R execute(final T theContext);
}
