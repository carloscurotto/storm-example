package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;

/**
 * Abstract parent class to all message classes on this package. Its main purpose is give a common type to all message
 * classes and ease the use of multiple types in maps and lists of messages.
 *
 * @author D540601
 */
public abstract class TradeMessage {
    protected String tradeNo = "";
    private Integer needReply = 0;
    protected String msgType;

    protected abstract void initMessageType();

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return needReply;
    }

    public void setNeedReply(Integer needReply) {
        this.needReply = needReply;
    }

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }
}
