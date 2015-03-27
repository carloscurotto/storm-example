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
    private final static String MESSAGE_TYPE = "SEMSAdapter.Link.BankStatus";

    /**
     * Status date format for normal trade messages.
     */
    private static final String STATUS_DATE_FORMAT = "yyyyMMdd_HHmmssSSS";

    /**
     * Default service name for normal trade messages.
     */
    private static final String DEFAULT_SERVICE_NAME = "SWFT";
    
    /**
     * Default origin application name for normal messages.
     */
    private static final String DEFAULT_ORIGIN_NAME = "IBOAPP";
    
    /**
     * Default origin reference for normal messages
     */
    private static final String DEFAULT_ORIGIN_REF = "SEMS";
    
    private String instNumber;
    private String statusDate;
    private String serviceName;
    private String statusCode;
    private NormalTradeNarrative narrative;

    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public NormalTradeStatusMessage(){}
    
    /**
     * Creates a normal trade update status message and adds it to messages parameter with key tradeReference.
     * Additionally it may create an external comment narrative and add it to the message itself.
     * 
     * @param {@link UpdateRow} with the required column values to build this NormalTradeStatusMessage. It must contain:
     *        tradeNo, externalComments, instNumber, statusCode, service and userId.
     */
    public NormalTradeStatusMessage(final UpdateRow theUpdateRow) {
        super(theUpdateRow, MESSAGE_TYPE);
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        Long theInstNumber = (Long) theUpdateRow.getUpdateColumnValue("instNumber");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        String theService = (String) theUpdateRow.getUpdateColumnValue("service");
        
        Validate.notNull(theInstNumber, "The instNumber cannot be null");
        Validate.notBlank(theStatusCode, "The statusCode cannot be blank");

        instNumber = theInstNumber.toString();
        SimpleDateFormat sdf = new SimpleDateFormat(STATUS_DATE_FORMAT);
        statusCode = theStatusCode;
        statusDate = sdf.format(new Date());

        if (StringUtils.isNotBlank(theExternalComments)) {
            narrative = new NormalTradeNarrative(theExternalComments);
        }
        
        serviceName = defaultServiceNameIfBlank(theService);
    }
    
    private String defaultServiceNameIfBlank(final String theService) {
        if(StringUtils.isNotBlank(theService)) {
            return theService;
        } else {
            return DEFAULT_SERVICE_NAME; 
        }
    }

    @XmlElement(name = "originName")
    public String getOriginName() {
        return DEFAULT_ORIGIN_NAME;
    }

    @XmlElement(name = "originRef")
    public String getOriginRef() {
        return DEFAULT_ORIGIN_REF;
    }

    @XmlElement(name = "instNumber")
    public String getInstNumber() {
        return instNumber;
    }

    @XmlElement(name = "statusDate")
    public String getStatusDate() {
        return statusDate;
    }

    @XmlElement(name = "serviceName")
    public String getServiceName() {
        return serviceName;
    }

    @XmlElement(name = "statusCode")
    public String getStatusCode() {
        return statusCode;
    }

    @XmlElement(name = "tradeNo")
    public String getTradeNumber() {
        return super.getTradeNumber();
    }

    @XmlElement(name = "userId")
    public String getUserId() {
        return super.getUserId();
    }

    @XmlElement(name = "Narrative")
    public NormalTradeNarrative getNarrative() {
        return narrative;
    }
}