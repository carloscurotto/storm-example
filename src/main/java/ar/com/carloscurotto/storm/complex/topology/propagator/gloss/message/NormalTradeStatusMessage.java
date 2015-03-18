package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "IBOMsg")
public class NormalTradeStatusMessage extends TradeMessage {

    private String msgType = "SEMSAdapter.Link.BankStatus";
    private Integer needReply = 0;
    private String originName = "IBOAPP";
    private String originRef = "SEMS";
    private String instNumber;
    private String statusDate;
    private String serviceName = "SWFT";
    private String statusCode;
    private String tradeNo;
    private String userId;
    private Narrative narrative;

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

    @XmlElement(name = "originName")
    public String getOriginName() {
        return originName;
    }

    public void setOriginName(String originName) {
        this.originName = originName;
    }

    @XmlElement(name = "originRef")
    public String getOriginRef() {
        return originRef;
    }

    public void setOriginRef(String originRef) {
        this.originRef = originRef;
    }

    @XmlElement(name = "instNumber")
    public String getInstNumber() {
        return instNumber;
    }

    public void setInstNumber(String instNumber) {
        this.instNumber = instNumber;
    }

    @XmlElement(name = "statusDate")
    public String getStatusDate() {
        return statusDate;
    }

    public void setStatusDate(String statusDate) {
        this.statusDate = statusDate;
    }

    @XmlElement(name = "serviceName")
    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @XmlElement(name = "statusCode")
    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    @XmlElement(name = "tradeNo")
    public String getTradeNo() {
        return tradeNo;
    }

    public void setTradeNo(String tradeNo) {
        this.tradeNo = tradeNo;
    }

    @XmlElement(name = "userId")
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @XmlElement(name = "Narrative")
    public Narrative getNarrative() {
        return narrative;
    }

    public void setNarrative(Narrative narrative) {
        this.narrative = narrative;
    }
}