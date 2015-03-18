package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.Map;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception.GlossException;

/**
 * Propagates and update to the GLOSS external system.
 *
 * @author D540601
 */
public class GlossUpdatePropagator extends AbstractUpdatePropagator {

    /**
     * serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Message builder.
     */
    private MessageBuilder messageBuilder;

    /**
     * Message sender.
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
     * Propagates the changes in the update object to the Gloss system.
     * @param theUpdate the Update to propagate. It cannot be null.
     */
    @Override
    protected ResultRow doExecute(UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The update cannot be null.");
        try {
            propagateRow(theContext.getParameters(), theContext.getRow());
        } catch( Throwable t) {
            return new ResultRow(theContext.getRow().getId(),ResultRowStatus.FAILURE, t.getMessage());
        }
        
        return new ResultRow(theContext.getRow().getId(),ResultRowStatus.SUCCESS, ResultRowStatus.SUCCESS.toString());
    }

    /**
     * Propagates the changes in the row to the Gloss system.
     * @param theParameters the parameters for this update. It cannot be null.
     * @param theUpdateRow the row to propagate. It cannot be null.
     */
    protected void propagateRow(final Map<String, Object> theParameters, UpdateRow theUpdateRow) {
        Messages messages = messageBuilder.build(theParameters, theUpdateRow);
        messageSender.execute(messages);
    }
}