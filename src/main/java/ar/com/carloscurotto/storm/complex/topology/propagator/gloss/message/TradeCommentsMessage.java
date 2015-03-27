package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

/**
 * Represents the information for comments for update messages. It is intended to be marshalled into XML string using
 * the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class TradeCommentsMessage extends TradeMessage {
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
    private TradeCommentsNarrative narrative;

    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public TradeCommentsMessage(){}
    
    /**
     * Builds an update comment message with the given {@link UpdateRow}.
     * 
     * @param {@link UpdateRow} with the needed column values to construct this TradeCommentsMessage. 
     * It must contain: internalComments.
     */
    public TradeCommentsMessage(final UpdateRow theUpdateRow) {
        super(theUpdateRow, MESSAGE_TYPE);
        String theInternalComments = (String) theUpdateRow.getUpdateColumnValue("internalComments");
        narrative = new TradeCommentsNarrative(theInternalComments);
    }

    @XmlElement(name = "TradeNo")
    public String getTradeNumber() {
        return super.getTradeNumber();
    }

    @XmlElement(name = "UserName")
    public String getUserName() {
        return super.getUserId();
    }

    @XmlElement(name = "EventCode")
    public String getEventCode() {
        return DEFAULT_EVENT_CODE;
    }

    @XmlElement(name = "EventType")
    public String getEventType() {
        return DEFAULT_EVENT_TYPE;
    }

    @XmlElement(name = "Narratives")
    public TradeCommentsNarrative getNarrative() {
        return narrative;
    }
}