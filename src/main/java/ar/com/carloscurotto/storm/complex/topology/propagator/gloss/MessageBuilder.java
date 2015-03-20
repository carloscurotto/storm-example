package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExcpTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeNarrative;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeCommentsMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionTradeNarrative;

/**
 * Builds normal, exception and comment trade messages, validating that the
 * input data for construction is correct for every case.
 *
 * @author D540601
 */
public class MessageBuilder {

    /**
     * Status date format for normal trade messages.
     */
    private static final String STATUS_DATE_FORMAT = "yyyyMMdd_HHmmssSSS";

    private static final String SCLT_NARRATIVE_CODE = "SCLT";
    private static final String ISTS_NARRATIVE_CODE = "ISTS";

    public Messages build(final Map<String, Object> theParameters, final UpdateRow updateRow) {
        Messages messages = new Messages();
        messages.setMainMessage(buildTrade(theParameters, updateRow));
        messages.setCommentMessage(buildComment(theParameters, updateRow));
        return messages;
    }

    /**
     * Builds a normal trade or exception trade according to updateStatus and exceptionTrade flags.
     */
    public TradeMessage buildTrade(final Map<String, Object> theParameters, final UpdateRow theUpdateRow) {
        TradeMessage result = null;
        Boolean updateStatus = (Boolean)theParameters.get("updateStatus");
        Boolean exceptionTrade = (Boolean)theParameters.get("exceptionTrade");
        String userId = (String)theParameters.get("userId"); 
        if (updateStatus) {
            if (exceptionTrade) {
                result = buildExceptionTradeAndUpdateStatus(userId, theUpdateRow);
            } else {
                result = buildNormalTradeAndUpdateStatus(userId, theUpdateRow);
            }
        }
        return result;
    }

    /**
     * Builds a comment message.
     */
    public TradeMessage buildComment(final Map<String, Object> theParameters, final UpdateRow theUpdateRow) {
      TradeMessage result = null;  
      Boolean updateIntComments = (Boolean)theParameters.get("updateInternalComment"); 
      if (updateIntComments) {
            result = buildUpdateComments((String)theParameters.get("userId"), theUpdateRow);
      }
      return result;
    }

    /**
     * Builds an update comment message and adds its to the messages parameter with tradeRef as key.
     */
    protected TradeMessage buildUpdateComments(final String theUserId, final UpdateRow theUpdateRow) {
        String tradeNo = (String)theUpdateRow.getUpdateColumnValue("tradeNo");
        String internalComments = (String)theUpdateRow.getUpdateColumnValue("internalComments");
        
        Validate.notBlank(theUserId, "The userId cannot be blank");
        Validate.notBlank(tradeNo, "tradeNo cannot be blank");
        Validate.notBlank(internalComments, "internalComments cannot be blank");

        ExceptionTradeNarrative narrative = new ExceptionTradeNarrative();
        narrative.setNarrativeCode1(NormalTradeNarrative.INTERNAL_NARRATIVE_CODE);
        narrative.setNarrativeText1(internalComments);
        narrative.setNoOfNarratives(1);

        TradeCommentsMessage comments = new TradeCommentsMessage();
        comments.setTradeNo(tradeNo);
        comments.setUserName(theUserId);
        comments.setNarrative(narrative);

        return comments;
    }

    /**
     * Creates a normal trade update status message and adds it to messages parameter with key tradeReference.
     * Additionally it may create an external comment narrative and add it to the message itself.
     */
    protected TradeMessage buildNormalTradeAndUpdateStatus(final String theUserId, final UpdateRow theUpdateRow) {
        String tradeNo = (String)theUpdateRow.getUpdateColumnValue("tradeNo");
        String externalComments = (String)theUpdateRow.getUpdateColumnValue("externalComments");
        Long instNumber = (Long)theUpdateRow.getUpdateColumnValue("instNumber");
        String statusCode = (String)theUpdateRow.getUpdateColumnValue("statusCode");
        String service = (String)theUpdateRow.getUpdateColumnValue("service");
        
        Validate.notBlank(theUserId, "The userId cannot be blank");
        Validate.notBlank(tradeNo, "tradeNo cannot be blank");
        Validate.notNull(instNumber, "instNumber cannot be null");
        Validate.notBlank(statusCode, "statusCode cannot be blank");

        NormalTradeStatusMessage msg = new NormalTradeStatusMessage();
        msg.setInstNumber(instNumber.toString());
        SimpleDateFormat sdf = new SimpleDateFormat(STATUS_DATE_FORMAT);
        msg.setTradeNo(tradeNo);
        msg.setStatusCode(statusCode);
        msg.setUserId(theUserId);
        msg.setStatusDate(sdf.format(new Date()));

        if (StringUtils.isNotBlank(externalComments)) {
            NormalTradeNarrative narrative = new NormalTradeNarrative();
            narrative.setNarrativeCode(NormalTradeNarrative.EXTERNAL_NARRATIVE_CODE);
            narrative.setNarrativeText(externalComments);
            msg.setNarrative(narrative);
        }

        if (StringUtils.isNotBlank(service)) {
            msg.setServiceName(service);
        }

        return msg;
    }

    /**
     * Creates an exception trade message and adds it to the messages object using tradeRef as key.
     *
     */
    protected TradeMessage buildExceptionTradeAndUpdateStatus(final String theUserId, final UpdateRow theUpdateRow) {
        String tradeNo = (String)theUpdateRow.getUpdateColumnValue("tradeNo");
        String externalComments = (String)theUpdateRow.getUpdateColumnValue("externalComments");
        String bankStatus = (String)theUpdateRow.getUpdateColumnValue("statusCode");
      
        Validate.notBlank(theUserId, "userId cannot be blank");
        Validate.notBlank(tradeNo, "tradeNo cannot be blank");

        Integer numberOfNarratives = 0;

        ExceptionTradeNarrative exceptionNarrative = new ExceptionTradeNarrative();

        if (StringUtils.isNotBlank(externalComments)) {
            numberOfNarratives++;
            exceptionNarrative.setNarrativeCode1(SCLT_NARRATIVE_CODE);
            exceptionNarrative.setNarrativeText1(externalComments);
        }

        if (StringUtils.isNotBlank(bankStatus)) {
            numberOfNarratives++;
            if (StringUtils.isNotBlank(externalComments)) {
                exceptionNarrative.setNarrativeCode2(ISTS_NARRATIVE_CODE);
                exceptionNarrative.setNarrativeText2(bankStatus);
            } else {
                exceptionNarrative.setNarrativeCode1(ISTS_NARRATIVE_CODE);
                exceptionNarrative.setNarrativeText1(bankStatus);
            }
        }

        exceptionNarrative.setNoOfNarratives(numberOfNarratives);

        ExcpTradeStatusMessage exceptionMessage = new ExcpTradeStatusMessage();
        exceptionMessage.setTradeNo(tradeNo);
        exceptionMessage.setUserName(theUserId);
        exceptionMessage.setNarrative(exceptionNarrative);

        return exceptionMessage;
    }
}