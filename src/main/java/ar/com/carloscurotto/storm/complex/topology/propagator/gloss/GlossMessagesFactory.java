package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder.GlossMessageBuilder;

/**
 * Factory of {@link GlossMessage}s that will be propagated into an external system as XML.
 *
 * @author D540601
 *
 */
public class GlossMessagesFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<GlossMessageBuilder> builders;

    /**
     * Constructs a factory of {@link GlossMessage}s with the given list of builders.
     *
     * @param theBuilders
     *            the builders that will be used for creating a list of {@link GlossMessage}s.
     *
     */
    public GlossMessagesFactory(final List<GlossMessageBuilder> theBuilders) {
        Validate.notEmpty(theBuilders, "The gloss message builders list cannot be null nor empty.");
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
        for (GlossMessageBuilder builder : builders) {
            CollectionUtils.addIgnoreNull(glossMessages, builder.build(theContext));
        }
        return glossMessages;
    }
}
