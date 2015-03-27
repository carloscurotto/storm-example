package ar.com.carloscurotto.storm.complex.service;

import com.google.common.base.Preconditions;

/**
 * This abstract class guarantees that a caller cannot execute open more than once. It delegates open and close
 * implementations to its subclasses.
 *
 * @author O605461, W506376
 */
public abstract class OpenAwareBean {

    private boolean isOpen;

    /**
     * It opens this bean. It is not idempotent since it will raise an {@link IllegalStateException} if it is already
     * opened.
     *
     * @throws {@link IllegalStateException} if this class is already opened.
     */
    public final void open() {
        Preconditions.checkState(!isOpen(), "The bean is already opened. It cannot be opened more than once.");
        doOpen();
        isOpen = true;
    }

    /**
     * Executes all the necessary steps for this class to be opened.
     */
    protected abstract void doOpen();

    /**
     * Checks whether this class is open or not.
     *
     * @return true if this class is open. False otherwise.
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * This method should be called in order to close this bean. It is idempotent.
     */
    public final void close() {
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
     * Validates whether this bean is opened or not.
     */
    protected final void validateIsOpened() {
        Preconditions.checkState(isOpen(), "This bean is not opened");
    }
}
