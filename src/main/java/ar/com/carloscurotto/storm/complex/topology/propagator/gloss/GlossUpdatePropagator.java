package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwarePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * Propagates row information through messaging to the Gloss external system.
 *
 * @author D540601
 */
public class GlossUpdatePropagator extends
        OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> {

    /**
     * serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Builds the xml string that compose the messages that are going to be sent to the Gloss external system.
     */
    private GlossMessageBuilder messageBuilder;

    /**
     * Sends the strings that composes the messages to the gloss external system.
     */
    private GlossMessageProducer messageSender;

    /**
     * Creates a {@link GlossUpdatePropagator} for the given message sender and builder.
     *
     * @param theMessageSender
     *            the given message sender. It can not be null.
     * @param theMessageBuilder
     *            the given message builder. It can not be null.
     */
    public GlossUpdatePropagator(final GlossMessageProducer theMessageSender,
            final GlossMessageBuilder theMessageBuilder) {
        Validate.notNull(theMessageSender, "The message sender can not be null.");
        Validate.notNull(theMessageBuilder, "The message builder can not be null.");
        messageSender = theMessageSender;
        messageBuilder = theMessageBuilder;
    }

    /**
     * See {@link com.jpmc.cib.csw.adp.update.service.Openable#open()}
     */
    @Override
    protected void doOpen() {
        messageSender.open();
    }

    /**
     * See {@link com.jpmc.cib.csw.adp.update.service.Closeable#close()}
     */
    @Override
    protected void doClose() {
        messageSender.close();
    }

    /**
     * Propagates the changes in the update object to the Gloss system.
     *
     * @param theContext
     *            the execution context that contains the necessary information to propagate gloss messages. It cannot
     *            be null.
     * @return the result of the updates propagation. It is never null.
     */
    @Override
    protected UpdatePropagatorResult doPropagate(UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The context cannot be null.");
        try {
            messageSender.send(messageBuilder.build(theContext));
            return UpdatePropagatorResult.createSuccess("SUCCESS");
        } catch (Exception e) {
            return UpdatePropagatorResult.createFailure(e.getMessage());
        }
    }

}