package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExcpTradeStatusMessage;
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
     * {@link ExcpTradeStatusMessage} and a {@link TradeCommentsMessage}.
     * 
     * @param theParameters
     *            a {@link java.util.Map<String, Object>}. It must have as a keys "updateStatus" as a boolean and
     *            "updateInternalComment" as a boolean.
     * @param theUpdateRow
     *            {@link UpdateRow} the row which state should be translated into string messages.
     * @return a {@link List<TradeMessage>} with the built messages. It can have from zero to two messages, according to
     *         the input parameters (see this' method documentation).
     */
    public List<TradeMessage> build(final Map<String, Object> theParameters,
            final UpdateRow theUpdateRow) {
        List<TradeMessage> messages = new ArrayList<TradeMessage>();

        Boolean updateStatus = (Boolean) theParameters.get("updateStatus");
        if (updateStatus) {
            messages.add(buildTrade(theParameters, theUpdateRow));
        }

        Boolean updateIntComments = (Boolean) theParameters.get("updateInternalComment");
        if (updateIntComments) {
            messages.add(new TradeCommentsMessage(theUpdateRow));
        }
        return messages;
    }

    /**
     * Builds a {@link TradeMessage} that could be either a {@link ExcpTradeStatusMessage} or a
     * {@link NormalTradeStatusMessage} according to the input parameters.
     * 
     * @param theParameters
     *            a {@link Map<String, Object>}. It must contain "exceptionTrade" as a boolean.
     * @param theUpdateRow
     *            {@link UpdateRow} the row which state should be translated into string messages.
     * @return a {@link TradeMessage} that can be either a {@link ExcpTradeStatusMessage} or
     *         {@link NormalTradeStatusMessage}
     */
    private TradeMessage buildTrade(final Map<String, Object> theParameters,
            final UpdateRow theUpdateRow) {
        TradeMessage result = null;

        Boolean exceptionTrade = (Boolean) theParameters.get("exceptionTrade");
        if (exceptionTrade) {
            result = new ExcpTradeStatusMessage(theUpdateRow);
        } else {
            result = new NormalTradeStatusMessage(theUpdateRow);
        }

        return result;
    }
}