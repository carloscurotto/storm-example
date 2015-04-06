package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

/**
 * Builds the different kind of {@link GlossMessage}s that will be propagated into Gloss subsystems.
 *
 * @author D540601
 */
public abstract class GlossMessageBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    protected Boolean isParameterValueTrue(final Map<String, Object> parameters, final String theParameterName) {
        Object value = parameters.get(theParameterName);
        Validate.notNull(value, "The value for the parameter: " + theParameterName + " cannot be null.");
        Validate.isTrue(Boolean.class.isInstance(value), "The value for the parameter: " + theParameterName
                + " is not Boolean");
        return ((Boolean) value);
    }

    /**
     * Checks whether this builder has enough information to construct the message.
     *
     * @param parameters
     *            the parameters with the data to decide whether this builder has to execute
     *            {@link GlossMessageBuilder#doBuild(UpdateRow)}.
     * @return true if this builder has to build a message. False otherwise.
     */
    protected abstract boolean shouldBuild(final Map<String, Object> parameters);

    /**
     * Builds a {@link GlossMessage} with the given update row.
     *
     * @param theUpdateRow
     *            the update row with the data to build a {@link GlossMessage}. It cannot be null.
     * @return a {@link GlossMessage} instance. It is never null.
     */
    protected abstract GlossMessage doBuild(final UpdateRow theUpdateRow);

    /**
     * Builds a message that will be propagated using the information contained in the given {@link UpdateRow}.
     *
     * @param theContext
     *            the context with the information for the update. It cannot be null.
     * @return a {@link GlossMessage} with the update information that will be propagated. It may be null if there is no
     *         information for this builder.
     */
    public final GlossMessage build(final UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The update propagator context cannot be null");
        GlossMessage glossMessage = null;
        if (shouldBuild(theContext.getParameters())) {
            glossMessage = doBuild(theContext.getRow());
        }
        return glossMessage;
    }

}