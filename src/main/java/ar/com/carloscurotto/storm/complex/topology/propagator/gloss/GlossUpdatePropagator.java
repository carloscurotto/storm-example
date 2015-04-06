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
public class GlossUpdatePropagator extends OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the XML strings with the messages that are going to be sent to the Gloss external system.
     */
    private GlossMessagesFactory messageFactory;

    /**
     * Sends the strings that composes the messages to the gloss external system.
     */
    private GlossMessageProducer messageProducer;

    /**
     * Creates a {@link GlossUpdatePropagator} for the given message sender and factory.
     *
     * @param theMessageFactory
     *            the given message factory. It can not be null.
     * @param theMessageProducer
     *            the given message producer. It can not be null.
     */
    public GlossUpdatePropagator(final GlossMessagesFactory theMessageFactory, final GlossMessageProducer theMessageProducer) {
        Validate.notNull(theMessageFactory, "The message factory can not be null.");
        Validate.notNull(theMessageProducer, "The message producer can not be null.");
        messageFactory = theMessageFactory;
        messageProducer = theMessageProducer;
    }

    @Override
    protected void doOpen() {
        messageProducer.open();
    }

    @Override
    protected void doClose() {
        messageProducer.close();
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
            messageProducer.send(messageFactory.create(theContext));
            return UpdatePropagatorResult.createSuccess("SUCCESS");
        } catch (Exception e) {
            return UpdatePropagatorResult.createFailure(e.getMessage());
        }
    }

}