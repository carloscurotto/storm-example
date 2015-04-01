package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeCommentsMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

/**
 * Builds normal, exception and comment trade messages, validating that the input data for construction is correct for
 * every case.
 *
 * @author D540601
 */
public class GlossMessageBuilder {

    public List<TradeMessage> build(final UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The context cannot be null.");
        final List<TradeMessage> messages = new ArrayList<TradeMessage>();
        addTradeStatusMessage(theContext, messages);
        addTradeCommentsMessage(theContext, messages);
        return messages;
    }

    private void addTradeStatusMessage(final UpdatePropagatorContext theContext, final List<TradeMessage> messages) {
        Boolean updateStatus = (Boolean) theContext.getValueForParameter("updateStatus");
        Validate.notNull(updateStatus, "The update status cannot be null.");
        if (updateStatus) {
            messages.add(createTradeStatusMessage(theContext));
        }
    }

    private void addTradeCommentsMessage(final UpdatePropagatorContext theContext, final List<TradeMessage> messages) {
        Boolean updateIntComments = (Boolean) theContext.getValueForParameter("updateInternalComment");
        Validate.notNull(updateIntComments, "updateIntComments cannot be null.");
        if (updateIntComments) {
            messages.add(createTradeCommentsMessage(theContext));
        }
    }

    private TradeCommentsMessage createTradeCommentsMessage(final UpdatePropagatorContext theContext) {
        return new TradeCommentsMessage(theContext.getRow());
    }

    private TradeMessage createTradeStatusMessage(final UpdatePropagatorContext theContext) {
        Boolean exceptionTrade = (Boolean) theContext.getValueForParameter("exceptionTrade");
        Validate.notNull(exceptionTrade, "exceptionTrade cannot be null.");
        if (exceptionTrade) {
            return new ExceptionTradeStatusMessage(theContext.getRow());
        } else {
            return new NormalTradeStatusMessage(theContext.getRow());
        }
    }

}