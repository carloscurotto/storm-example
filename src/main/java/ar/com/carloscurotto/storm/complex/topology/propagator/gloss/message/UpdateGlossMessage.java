package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * Represents an update status message.
 *
 * @author D540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class UpdateGlossMessage implements GlossMessage {

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
    private String statusCode;
    private String serviceName;
    private String statusDate;
    private String userId;
    private String tradeNumber;
    private Narrative narrative;

    /**
     * This constructor should not be used. It is only here for the use of the JAXB framework.
     */
    @Deprecated
    public UpdateGlossMessage() {
    }

    public UpdateGlossMessage(final Long theInstNumber, final String theStatusCode, final String theService,
            final String theExternalComments, final String theUserId, final String theTradeNumber) {
        Validate.notNull(theInstNumber, "The instNumber cannot be null.");
        Validate.notBlank(theStatusCode, "The statusCode cannot be blank.");
        Validate.notBlank(theUserId, "The user id cannot be blank.");
        Validate.notBlank(theTradeNumber, "The trade number cannot be blank.");

        instNumber = theInstNumber.toString();
        statusCode = theStatusCode;
        setDefaultServiceNameIfBlank(theService);
        setStatusDate();
        setNarrative(theExternalComments);
    }

    private void setDefaultServiceNameIfBlank(final String theService) {
        if (StringUtils.isNotBlank(theService)) {
            serviceName = theService;
        } else {
            serviceName = DEFAULT_SERVICE_NAME;
        }
    }

    private void setStatusDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(STATUS_DATE_FORMAT);
        statusDate = simpleDateFormat.format(new Date());
    }

    private void setNarrative(final String theExternalComments) {
        if (StringUtils.isNotBlank(theExternalComments)) {
            narrative = new Narrative(theExternalComments);
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

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return MESSAGE_TYPE;
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

    @XmlElement(name = "userId")
    public String getUserId() {
        return userId;
    }

    @XmlElement(name = "tradeNo")
    public String getTradeNumber() {
        return tradeNumber;
    }

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return 0;
    }

    @XmlElement(name = "Narrative")
    public Narrative getNarrative() {
        return narrative;
    }

    @XmlRootElement(name = "Narrative")
    private static class Narrative {

        /**
         * Represents the code for the update messages' external comments.
         */
        private static final String EXTERNAL_NARRATIVE_CODE = "ERR";

        /**
         * Represents the comment code for this narrative.
         */
        private String narrativeCode;

        /**
         * Represent the comment text for this narrative.
         */
        private String narrativeText;

        @SuppressWarnings("unused")
        @Deprecated
        public Narrative() {
        }

        /**
         * Constructs the NormalTradeNarrative with the given external comments.
         *
         * @param externalComments
         *            a String with the external comment. It doesn't require to have content. It can be null or blank.
         */
        public Narrative(final String externalComments) {
            this.narrativeCode = EXTERNAL_NARRATIVE_CODE;
            this.narrativeText = externalComments;
        }

        @XmlElement(name = "narrativeCode")
        public String getNarrativeCode() {
            return narrativeCode;
        }

        @XmlElement(name = "narrativeText")
        public String getNarrativeText() {
            return narrativeText;
        }
    }

}