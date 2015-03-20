package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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