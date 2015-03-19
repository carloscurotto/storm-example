package ar.com.carloscurotto.storm.complex.service;

import com.google.common.base.Preconditions;

/**
 * Base service for all the implementations that wants to guarantee that the service is open before performing its
 * operation.
 *
 * @author O605461, W506376
 * @param <T>
 *            the execution context's type.
 * @param <R>
 *            the execution result's type.
 */
public abstract class OpenAwareService<T, R> implements Service<T, R> {

    private boolean isOpen;

    @Override
    public void open() {
        Preconditions.checkState(!isOpen(),
                "The service is already opened. Can not open more than once.");
        doOpen();
        isOpen = true;
    }

    /**
     * This method will be called before marking the service as opened to give the user the opportunity to perform the
     * corresponding open logic.
     */
    protected abstract void doOpen();

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * This method will be called before marking the service as closed to give the user the opportunity to perform the
     * corresponding close logic.
     */
    @Override
    public void close() {
        if (isOpen()) {
            doClose();
            isOpen = false;
        }
    }

    protected abstract void doClose();

    @Override
    public final R execute(final T theContext) {
        Preconditions.checkState(isOpen(),
                "Can not execute a closed service.  Please, open it first.");
        return this.doExecute(theContext);
    }

    /**
     * This method will be called after ensuring that the service is already opened. This method should implement the
     * actual service's execution.
     *
     * @param theContext
     *            the execution context.
     * @return the execution result.
     */
    protected abstract R doExecute(final T theContext);
}
