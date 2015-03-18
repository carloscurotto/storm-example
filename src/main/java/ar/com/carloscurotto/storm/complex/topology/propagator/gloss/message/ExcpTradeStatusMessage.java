package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This is used to generate both - Exeption Trade Status Message & Comments message.
 *
 * @author e208105
 *
 */
@XmlRootElement(name = "IBOMsg")
public class ExcpTradeStatusMessage extends TradeMessage {

    private String userName;

    private String msgType = "SEMSAdapter.Trade.AddEvent";

    private String tradeNo = "";

    private String eventCode = "SCRT"; // defaulted for Status message

    private String eventType = "TENR"; // defaulted for Status message

    private TradeNarrative narrative;

    private Integer needReply = 0;

    @XmlElement(name = "UserName")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
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
    public TradeNarrative getNarrative() {
        return this.narrative;
    }

    public void setNarrative(TradeNarrative narrative) {
        this.narrative = narrative;
    }

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return needReply;
    }

    public void setNeedReply(Integer needReply) {
        this.needReply = needReply;
    }
}