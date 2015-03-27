package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

/**
 * Represents the information for comments for update messages. It is intended to be marshalled into XML string using
 * the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class TradeCommentsMessage extends TradeMessage {
    /**
     * Message type for this trade message.
     */
    private final String MESSAGE_TYPE = "SEMSAdapter.Trade.AddEvent";

    private String userName;
    private String eventCode = "SEMT";
    private String eventType = "SEMI";
    private ExceptionTradeNarrative narrative;

    /**
     * Builds an update comment message with the given {@link UpdateRow}.
     * 
     * @param {@link UpdateRow} with the needed column values to construct this TradeCommentsMessage. It must contain:
     *        tradeNo, internalComments and userId.
     */
    public TradeCommentsMessage(final UpdateRow theUpdateRow) {
        String theTradeNo = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theInternalComments = (String) theUpdateRow.getUpdateColumnValue("internalComments");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");

        Validate.notBlank(theUserId, "The userId cannot be blank");
        Validate.notBlank(theTradeNo, "tradeNo cannot be blank");
        Validate.notBlank(theInternalComments, "internalComments cannot be blank");

        tradeNo = theTradeNo;
        userName = theUserId;
        narrative = new ExceptionTradeNarrative(theInternalComments);
    }

    @Override
    protected void initMessageType() {
        setMsgType(MESSAGE_TYPE);
    }

    @XmlElement(name = "TradeNo")
    public String getTradeNo() {
        return tradeNo;
    }

    public void setTradeNo(String tradeNo) {
        this.tradeNo = tradeNo;
    }

    @XmlElement(name = "UserName")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userId) {
        this.userName = userId;
    }

    @XmlElement(name = "EventCode")
    public String getEventCode() {
        return eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }

    @XmlElement(name = "EventType")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @XmlElement(name = "Narratives")
    public ExceptionTradeNarrative getNarrative() {
        return narrative;
    }

    public void setNarrative(ExceptionTradeNarrative narrative) {
        this.narrative = narrative;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}