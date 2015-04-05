package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.Validate;

/**
 * Represents the information for comments messages.
 *
 * @author D540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class CommentGlossMessage implements GlossMessage {

    /**
     * Message type for this trade message.
     */
    private final static String MESSAGE_TYPE = "SEMSAdapter.Trade.AddEvent";

    /**
     * Default event code for trade comment messages.
     */
    private static final String DEFAULT_EVENT_CODE = "SEMT";

    /**
     * Default event type for trade comment messages.
     */
    private static final String DEFAULT_EVENT_TYPE = "SEMI";

    private String userId;
    private String tradeNumber;
    private Narrative narrative;

    /**
     * This constructor should not be used. It is only here for the use of the JAXB framework.
     */
    @Deprecated
    public CommentGlossMessage() {
    }

    public CommentGlossMessage(final String theInternalComments, final String theUserId, final String theTradeNumber) {
        Validate.notBlank(theUserId, "The user id cannot be blank.");
        Validate.notBlank(theTradeNumber, "The trade number cannot be blank.");
        userId = theUserId;
        tradeNumber = theTradeNumber;
        narrative = new Narrative(theInternalComments);
    }

    @XmlElement(name = "EventCode")
    public String getEventCode() {
        return DEFAULT_EVENT_CODE;
    }

    @XmlElement(name = "EventType")
    public String getEventType() {
        return DEFAULT_EVENT_TYPE;
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

    @XmlElement(name = "Narratives")
    public Narrative getNarrative() {
        return narrative;
    }

    @XmlRootElement(name = "Narratives")
    private static class Narrative {

        private static final String INTERNAL_NARRATIVE_CODE = "SINT";
        private String narrativeText1;

        @SuppressWarnings("unused")
        @Deprecated
        public Narrative() {
        }

        public Narrative(final String theInternalComments) {
            Validate.notBlank(theInternalComments, "The internal comments cannot be blank.");
            narrativeText1 = theInternalComments;
        }

        @XmlElement(name = "NoOfNarratives")
        public Integer getNoOfNarratives() {
            return 1;
        }

        @XmlElement(name = "NarrativeCode1")
        public String getNarrativeCode1() {
            return INTERNAL_NARRATIVE_CODE;
        }

        @XmlElement(name = "NarrativeText1")
        public String getNarrativeText1() {
            return narrativeText1;
        }
    }
}