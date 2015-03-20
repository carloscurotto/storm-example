package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Message class to express the information for normal trade status messages. It is intended to be marshalled into XML
 * string using the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class NormalTradeStatusMessage extends TradeMessage {
    /**
     * Message type for this trade message.
     */
    private final String MESSAGE_TYPE = "SEMSAdapter.Link.BankStatus";

    private String originName = "IBOAPP";
    private String originRef = "SEMS";
    private String instNumber;
    private String statusDate;
    private String serviceName = "SWFT";
    private String statusCode;
    private String userId;
    private NormalTradeNarrative narrative;

    @Override
    protected void initMessageType() {
        setMsgType(MESSAGE_TYPE);
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
    public NormalTradeNarrative getNarrative() {
        return narrative;
    }

    public void setNarrative(NormalTradeNarrative narrative) {
        this.narrative = narrative;
    }
}