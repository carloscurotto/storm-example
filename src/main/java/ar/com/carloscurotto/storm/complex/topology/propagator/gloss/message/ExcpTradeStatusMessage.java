package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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