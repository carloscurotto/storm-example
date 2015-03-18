package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception;

/**
 * Signals and error produced when sending a message through a jms queue.
 *
 * @author D540601
 */
public class GlossJmsException extends GlossException {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 1L;

    /**
     * See {@link GlossException#GlossException(String, Exception)}
     */
    public GlossJmsException(String theMessage, Exception theException) {
        super(theMessage, theException);
    }
}
