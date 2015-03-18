package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This is used for generating Normal Trade Comments message. There is a difference in XML structure for Normal Trade
 * Status & Comments message. Currently, comments XML structure is same for Normal Trade & Exception Trade. However, it
 * was kept separate.
 *
 * @author e208105
 *
 */
@XmlRootElement(name = "IBOMsg")
public class TradeCommentsMessage extends TradeMessage {
    private String msgType = "SEMSAdapter.Trade.AddEvent";

    private Integer needReply = 0;

    private String tradeNo;

    private String userName;

    private String eventCode = "SEMT";

    private String eventType = "SEMI";

    private TradeNarrative narrative;

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return needReply;
    }

    public void setNeedReply(Integer needReply) {
        this.needReply = needReply;
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
    public TradeNarrative getNarrative() {
        return narrative;
    }

    public void setNarrative(TradeNarrative narrative) {
        this.narrative = narrative;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}