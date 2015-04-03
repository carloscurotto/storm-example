package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder.GlossMessageBuilder;

/**
 * Factory of {@link GlossMessage}s that will be propagated into an external system as XML.
 *
 * @author D540601
 *
 */
public class GlossMessagesFactory {

    private Map<String, GlossMessageBuilder> builders;

    /**
     * Constructs a factory of {@link GlossMessage}s with the given map of builders.
     *
     * @param theBuilders
     *            the builders that will be used for creating a list of {@link GlossMessage}s.
     *
     */
    public GlossMessagesFactory(final Map<String, GlossMessageBuilder> theBuilders) {
        Validate.notEmpty(theBuilders, "The gloss message builders map cannot be null nor empty.");
        builders = theBuilders;
    }

    /**
     * Creates a list of {@link GlossMessage}s using the information from the parameters contained in the given context.
     *
     * @param theContext
     *            contains information about which messages should be created. It cannot be null. It must also has to
     *            have a non empty map of parameters.
     * @return a list of {@link GlossMessage}s. It may me empty if the given context does not provide correct
     *         information about the messages that has to be created.
     */
    public List<GlossMessage> create(final UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The update propagator context cannot be null.");
        Validate.isTrue(theContext.hasParameters(), "The update propagator context's parameter must not be empty.");

        return buildMessages(theContext);
    }

    private List<GlossMessage> buildMessages(final UpdatePropagatorContext theContext) {
        List<GlossMessage> glossMessages = new LinkedList<GlossMessage>();
        UpdateRow updateRow = theContext.getRow();
        if (isUpdateMessage(theContext)) {
            glossMessages.add(builders.get("update").build(updateRow));
        } else if (isExceptionMessage(theContext)) {
            glossMessages.add(builders.get("exception").build(updateRow));
        }
        if (isCommentMessage(theContext)) {
            glossMessages.add(builders.get("comment").build(updateRow));
        }
        return glossMessages;
    }

    private boolean isUpdateMessage(final UpdatePropagatorContext theContext) {
        return isParameterValueTrue(theContext, "update") && !isParameterValueTrue(theContext, "exceptionTrade");
    }

    private boolean isExceptionMessage(final UpdatePropagatorContext theContext) {
        return isParameterValueTrue(theContext, "update") && isParameterValueTrue(theContext, "exceptionTrade");
    }

    private boolean isCommentMessage(final UpdatePropagatorContext theContext) {
        return isParameterValueTrue(theContext, "updateInternalComment");
    }

    private Boolean isParameterValueTrue(final UpdatePropagatorContext theContext, final String theParameterName) {
        Object value = theContext.getValueForParameter(theParameterName);
        Validate.notNull(value, "The value for the parameter: " + theParameterName + " cannot be null.");
        Validate.isTrue(Boolean.class.isInstance(value), "The value for the parameter: " + theParameterName
                + " is not Boolean");
        return ((Boolean) value);
    }
}
