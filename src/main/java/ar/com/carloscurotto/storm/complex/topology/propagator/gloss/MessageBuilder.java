package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
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
public class MessageBuilder {

    /**
     * Builds the update message/s depending on the information in the given parameters. It can at most build two
     * messages: one main message that can be either a {@link NormalTradeStatusMessage} or a
     * {@link ExceptionTradeStatusMessage} and a {@link TradeCommentsMessage}.
     * 
     * @param theParameters
     *            a {@link java.util.Map<String, Object>}. It cannot be null. 
     *            It must have as a keys "updateStatus" as a boolean and updateInternalComment" as a boolean.
     * @param theUpdateRow
     *            {@link UpdateRow} the row which state should be translated into string messages.
     *            It cannot be null.
     * @return a {@link List<TradeMessage>} with the built messages. It can have from zero to two messages, according to
     *         the input parameters (see this' method documentation).
     */
    public List<TradeMessage> build(final Map<String, Object> theParameters,
            final UpdateRow theUpdateRow) {
        Validate.notNull(theParameters, "The parameters map cannot be null.");
        Validate.notNull(theUpdateRow, "The updateRow cannot be null.");
        
        List<TradeMessage> messages = new ArrayList<TradeMessage>();

        Boolean updateStatus = (Boolean) theParameters.get("updateStatus");
        Validate.notNull(updateStatus, "updateStatus cannot be null.");
        if (updateStatus) {
            messages.add(buildTrade(theParameters, theUpdateRow));
        }

        Boolean updateIntComments = (Boolean) theParameters.get("updateInternalComment");
        Validate.notNull(updateIntComments, "updateIntComments cannot be null.");
        if (updateIntComments) {
            messages.add(new TradeCommentsMessage(theUpdateRow));
        }
        return messages;
    }

    /**
     * Builds a {@link TradeMessage} that could be either a {@link ExceptionTradeStatusMessage} or a
     * {@link NormalTradeStatusMessage} according to the input parameters.
     * 
     * @param theParameters
     *            a {@link Map<String, Object>}. It cannot be null. It must contain "exceptionTrade" as a boolean.
     * @param theUpdateRow
     *            {@link UpdateRow} the row which state should be translated into string messages.
     *            It cannot be null.
     * @return a {@link TradeMessage} that can be either a {@link ExceptionTradeStatusMessage} or
     *         {@link NormalTradeStatusMessage}
     */
    private TradeMessage buildTrade(final Map<String, Object> theParameters,
            final UpdateRow theUpdateRow) {
        Validate.notNull(theParameters, "theParameters cannot be null");
        Validate.notNull(theUpdateRow, "theUpdateRow cannot be null");
        
        TradeMessage result = null;

        Boolean exceptionTrade = (Boolean) theParameters.get("exceptionTrade");
        Validate.notNull(exceptionTrade, "exceptionTrade cannot be null.");
        if (exceptionTrade) {
            result = new ExceptionTradeStatusMessage(theUpdateRow);
        } else {
            result = new NormalTradeStatusMessage(theUpdateRow);
        }

        return result;
    }
}