package ar.com.carloscurotto.storm.complex.service;

import com.google.common.base.Preconditions;

/**
 * Base class that provides a validation mechanism before executing the main action.
 *
 * @author O605461, W506376
 * @param <T>
 *            the execution context's type.
 * @param <R>
 *            the execution result's type.
 */
public abstract class OpenAwareBean<T, R> implements Closeable, Openable {

    private boolean isOpen;

    /**
     * Prepares this class to be run. This must be called once before calling {@link #execute()}. It is not idempotent
     * since it will raise an {@link IllegalStateException} if is already opened.
     * 
     * @throws {@link IllegalStateException} if this class is already opened.
     */
    @Override
    public void open() {
        Preconditions.checkState(!isOpen(), "The service is already opened. Cannot open more than once.");
        doOpen();
        isOpen = true;
    }

    /**
     * Executes all the necessary steps for this class to be in a consistent state before the caller can execute its
     * {@link #execute()} method.
     */
    protected abstract void doOpen();

    /**
     * Checks whether this class is open or not.
     * 
     * @return true if this class is open. False otherwise.
     */
    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * This method should be called by the caller any time after this class finishes executing. It is idempotent.
     */
    @Override
    public void close() {
        if (isOpen()) {
            doClose();
            isOpen = false;
        }
    }

    /**
     * Executes all the necessary tasks for freeing up resources.
     */
    protected abstract void doClose();

    /**
     * Performs the particular action associated to this class. This method is synchronous in that it will return the
     * control to the caller after the execution is completed.
     * 
     * @param theContext
     *            a {@code T} instance that serves as context. It can be null.
     * @return a {@code R} with the result of executing this method.
     */
    public final R execute(final T theContext) {
        Preconditions.checkState(isOpen(), "Can not execute a closed service.  Please, open it first.");
        return doExecute(theContext);
    }

    /**
     * This method will be called after ensuring that this class is already opened. It must never return null.
     *
     * @param theContext
     *            a {@code T} instance that serves as context. It can be null.
     * @return a {@code R} with the result of executing this method. It is never null.
     */
    protected abstract R doExecute(final T theContext);
}
