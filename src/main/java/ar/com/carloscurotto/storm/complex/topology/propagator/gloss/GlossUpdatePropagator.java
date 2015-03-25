package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * Propagates row information through messaging to the Gloss external system.
 *
 * @author D540601
 */
public class GlossUpdatePropagator extends AbstractUpdatePropagator {

    /**
     * serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Builds the xml string that compose the messages that are going to be sent to the Gloss external system.
     */
    private MessageBuilder messageBuilder;

    /**
     * Sends the strings that composes the messages to the gloss external system.
     */
    private MessageSender messageSender;

    /**
     * Creates a {@link GlossUpdatePropagator} for the given message sender and builder.
     *
     * @param theMessageSender
     *            the given message sender. It can not be null.
     * @param theMessageBuilder
     *            the given message builder. It can not be null.
     */
    public GlossUpdatePropagator(final MessageSender theMessageSender,
            final MessageBuilder theMessageBuilder) {
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
     * Propagates the changes in the row to the Gloss system.
     * 
     * @param theParameters
     *            the parameters for this update. It cannot be null.
     * @param theUpdateRow
     *            the row to propagate. It cannot be null.
     */
    private void propagateRow(final Map<String, Object> theParameters, UpdateRow theUpdateRow) {
        List<TradeMessage> messages = messageBuilder.build(theParameters, theUpdateRow);
        messageSender.execute(messages);
    }
    
    /**
     * Propagates the changes in the update object to the Gloss system.
     * 
     * @param theContext
     *            a {@link UpdatePropagatorContext} the context for this method. Provides the execution context and the
     *            parameters for this method. {@link UpdatePropagatorContext#getParameters()} and
     *            {@link UpdatePropagatorContext#getRow()} are used to create the messages to propagate. This parameter
     *            cannot be null.
     */
    @Override
    protected UpdatePropagatorResult doExecute(UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The update cannot be null.");
        try {
            propagateRow(theContext.getParameters(), theContext.getRow());
        } catch (Exception e) {
            return UpdatePropagatorResult.createFailure(e.getMessage());
        }

        return UpdatePropagatorResult.createSuccess("SUCCESS");
    }
}