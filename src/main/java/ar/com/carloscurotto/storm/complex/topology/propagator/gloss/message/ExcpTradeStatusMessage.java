package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

/**
 * This is used to generate Exception Trade Status Message XML. It is intended to be marshalled into XML string using
 * the JAXB framework.
 *
 * @author d540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class ExcpTradeStatusMessage extends TradeMessage {
    /**
     * Message type for this trade message.
     */
    private final String MESSAGE_TYPE = "SEMSAdapter.Trade.AddEvent";

    private String userName;
    private String eventCode = "SCRT"; // defaulted for Status message
    private String eventType = "TENR"; // defaulted for Status message
    private ExceptionTradeNarrative narrative;

    /**
     * Code for external comments in an exception trade messages.
     */
    private static final String SCLT_NARRATIVE_CODE = "SCLT";

    /**
     * Code for internal status for exception trades status messages.
     */
    private static final String ISTS_NARRATIVE_CODE = "ISTS";

    /**
     * Creates an exception trade message with the information on the give {@link UpdateRow} parameter.
     * 
     * @param 
     *            {@link UpdateRow} with the needed values to construct this ExcpTradeStatusMessage. It must contain
     *            values for: tradeNo, externalComments, statusCode and userId.
     */
    public ExcpTradeStatusMessage(final UpdateRow theUpdateRow) {
        String theTradeNo = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");

        Validate.notBlank(theUserId, "The userId cannot be blank");
        Validate.notBlank(theTradeNo, "The tradeNo cannot be blank");

        Integer numberOfNarratives = 0;

        ExceptionTradeNarrative exceptionNarrative = new ExceptionTradeNarrative();

        if (StringUtils.isNotBlank(theExternalComments)) {
            numberOfNarratives++;
            exceptionNarrative.setNarrativeCode1(SCLT_NARRATIVE_CODE);
            exceptionNarrative.setNarrativeText1(theExternalComments);
        }

        if (StringUtils.isNotBlank(theStatusCode)) {
            numberOfNarratives++;
            if (StringUtils.isNotBlank(theExternalComments)) {
                exceptionNarrative.setNarrativeCode2(ISTS_NARRATIVE_CODE);
                exceptionNarrative.setNarrativeText2(theStatusCode);
            } else {
                exceptionNarrative.setNarrativeCode1(ISTS_NARRATIVE_CODE);
                exceptionNarrative.setNarrativeText1(theStatusCode);
            }
        }

        exceptionNarrative.setNoOfNarratives(numberOfNarratives);

        tradeNo = theTradeNo;
        userName = theUserId;
        narrative = exceptionNarrative;
    }

    @Override
    protected void initMessageType() {
        setMsgType(MESSAGE_TYPE);
    }

    @XmlElement(name = "UserName")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @XmlElement(name = "TradeNo")
    public String getTradeNo() {
        return tradeNo;
    }

    public void setTradeNo(String tradeNo) {
        this.tradeNo = tradeNo;
    }

    @XmlElement(name = "EventCode")
    public String getEventCode() {
        return this.eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }

    @XmlElement(name = "EventType")
    public String getEventType() {
        return this.eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @XmlElement(name = "Narratives")
    public ExceptionTradeNarrative getNarrative() {
        return this.narrative;
    }

    public void setNarrative(ExceptionTradeNarrative narrative) {
        this.narrative = narrative;
    }
}