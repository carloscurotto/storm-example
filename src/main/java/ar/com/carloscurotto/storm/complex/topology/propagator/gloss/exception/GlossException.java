package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception;

import org.apache.commons.lang3.Validate;

/**
 * Parent class for exceptions caused by fails in the gloss submodule
 * @author D540601
 */
public abstract class GlossException extends RuntimeException {
    /**
     * serial version id for this exception.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * @param theException the original exception. It cannot be null.
     */
    public GlossException(final Exception theException) {
        super(theException);
        Validate.notNull(theException, "The exception cannot be null");
    }
    
    /**
     * Constructor.
     * @param theMessage the exception message. It cannot be blank.
     * @param theException the original exception. It cannot be blank.
     */
    public GlossException(final String theMessage, final Exception theException) {
        super(theMessage, theException);
        Validate.notBlank(theMessage, "The message cannot be blank");
        Validate.notNull(theException, "The exception cannot be null");
    }
}
