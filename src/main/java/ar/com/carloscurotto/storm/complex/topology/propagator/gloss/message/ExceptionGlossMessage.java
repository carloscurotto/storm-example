package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;

/**
 * Represents a message gloss exception message.
 *
 * @author d540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class ExceptionGlossMessage implements GlossMessage {

    private static final long serialVersionUID = 1L;

    /**
     * Message type for this trade message.
     */
    private static final String MESSAGE_TYPE = "SEMSAdapter.Trade.AddEvent";

    /**
     * Default event code for exception trade status messages.
     */
    private static final String EVENT_CODE = "SCRT";

    /**
     * Default event type for exception trade status messages.
     */
    private static final String EVENT_TYPE = "TENR";

    private String userId;
    private String tradeNumber;

    private Narrative narrative;

    /**
     * This constructor should not be used. It is only here for the use of the JAXB framework.
     */
    @Deprecated
    public ExceptionGlossMessage() {
    }

    public ExceptionGlossMessage(final String theExternalComments, final String theUserId, final String theTradeNumber,
            final String theStatusCode) {
        userId = theUserId;
        tradeNumber = theTradeNumber;
        narrative = new Narrative(theExternalComments, theStatusCode);
    }

    @XmlElement(name = "UserName")
    public String getUserId() {
        return userId;
    }

    @XmlElement(name = "TradeNo")
    public String getTradeNumber() {
        return tradeNumber;
    }

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return MESSAGE_TYPE;
    }

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return 0;
    }

    @XmlElement(name = "EventCode")
    public String getEventCode() {
        return EVENT_CODE;
    }

    @XmlElement(name = "EventType")
    public String getEventType() {
        return EVENT_TYPE;
    }

    @XmlElement(name = "Narratives")
    public Narrative getNarrative() {
        return narrative;
    }

    @XmlRootElement(name = "Narratives")
    private static class Narrative {

        /**
         * Code for external comments in an exception trade messages.
         */
        private static final String SCLT_NARRATIVE_CODE = "SCLT";

        /**
         * Code for internal status for exception trades status messages.
         */
        private static final String ISTS_NARRATIVE_CODE = "ISTS";

        private Integer numberOfNarratives;
        private String narrativeCode1;
        private String narrativeText1;
        private String narrativeCode2;
        private String narrativeText2;

        @SuppressWarnings("unused")
        @Deprecated
        public Narrative() {
        }

        public Narrative(final String theExternalComments, final String theStatusCode) {
            numberOfNarratives = 0;
            if (StringUtils.isNotBlank(theExternalComments)) {
                numberOfNarratives++;
                narrativeCode1 = SCLT_NARRATIVE_CODE;
                narrativeText1 = theExternalComments;
            }

            if (StringUtils.isNotBlank(theStatusCode)) {
                numberOfNarratives++;
                if (StringUtils.isNotBlank(theExternalComments)) {
                    narrativeCode2 = ISTS_NARRATIVE_CODE;
                    narrativeText2 = theStatusCode;
                } else {
                    narrativeCode1 = ISTS_NARRATIVE_CODE;
                    narrativeText1 = theStatusCode;
                }
            }
        }

        @XmlElement(name = "NoOfNarratives")
        public Integer getNoOfNarratives() {
            return this.numberOfNarratives;
        }

        @XmlElement(name = "NarrativeCode1")
        public String getNarrativeCode1() {
            return this.narrativeCode1;
        }

        @XmlElement(name = "NarrativeText1")
        public String getNarrativeText1() {
            return this.narrativeText1;
        }

        @XmlElement(name = "NarrativeCode2")
        public String getNarrativeCode2() {
            return this.narrativeCode2;
        }

        @XmlElement(name = "NarrativeText2")
        public String getNarrativeText2() {
            return this.narrativeText2;
        }
    }
}