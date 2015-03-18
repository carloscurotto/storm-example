package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception;


/**
 * Signals an error that occurs when marshalling data with JAXB framework classes.
 *
 * @author D540601
 */
public class GlossMarshalException extends GlossException {

    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 1L;

    /**
     * See {@link GlossException#GlossException(Exception)}
     */
    public GlossMarshalException(Exception exception) {
        super(exception);
    }
}
