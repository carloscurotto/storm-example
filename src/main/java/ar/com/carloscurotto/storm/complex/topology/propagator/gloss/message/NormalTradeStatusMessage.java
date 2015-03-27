package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

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

    /**
     * Status date format for normal trade messages.
     */
    private static final String STATUS_DATE_FORMAT = "yyyyMMdd_HHmmssSSS";

    private String originName = "IBOAPP";
    private String originRef = "SEMS";
    private String instNumber;
    private String statusDate;
    private String serviceName = "SWFT";
    private String statusCode;
    private String userId;
    private NormalTradeNarrative narrative;

    /**
     * Creates a normal trade update status message and adds it to messages parameter with key tradeReference.
     * Additionally it may create an external comment narrative and add it to the message itself.
     * 
     * @param {@link UpdateRow} with the required column values to build this NormalTradeStatusMessage. It must contain:
     *        tradeNo, externalComments, instNumber, statusCode, service and userId.
     */
    public NormalTradeStatusMessage(final UpdateRow theUpdateRow) {
        String theTradeNo = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        Long theInstNumber = (Long) theUpdateRow.getUpdateColumnValue("instNumber");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        String theService = (String) theUpdateRow.getUpdateColumnValue("service");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");

        Validate.notBlank(theUserId, "The userId cannot be blank");
        Validate.notBlank(theTradeNo, "The tradeNo cannot be blank");
        Validate.notNull(theInstNumber, "The instNumber cannot be null");
        Validate.notBlank(theStatusCode, "The statusCode cannot be blank");

        instNumber = theInstNumber.toString();
        SimpleDateFormat sdf = new SimpleDateFormat(STATUS_DATE_FORMAT);
        tradeNo = theTradeNo;
        statusCode = theStatusCode;
        userId = theUserId;
        statusDate = sdf.format(new Date());

        if (StringUtils.isNotBlank(theExternalComments)) {
            setNarrative(new NormalTradeNarrative(theExternalComments));
        }

        if (StringUtils.isNotBlank(theService)) {
            setServiceName(theService);
        }
    }

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