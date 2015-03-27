package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

/**
 * This is used to generate Exception Trade Status Message XML. It is intended to be marshalled into XML string using
 * the JAXB framework.
 *
 * @author d540601
 *
 */
@XmlRootElement(name = "IBOMsg")
public class ExceptionTradeStatusMessage extends TradeMessage {
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
    
    
    private ExceptionTradeNarrative narrative;

    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public ExceptionTradeStatusMessage(){}
    
    /**
     * Creates an exception trade message with the information on the give {@link UpdateRow} parameter.
     * 
     * @param 
     *            {@link UpdateRow} with the needed values to construct this ExcpTradeStatusMessage. It must contain
     *            values for: tradeNo, externalComments, statusCode and userId.
     */
    public ExceptionTradeStatusMessage(final UpdateRow theUpdateRow) {
        super(theUpdateRow, MESSAGE_TYPE);
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        
        narrative = new ExceptionTradeNarrative(theExternalComments, theStatusCode);
    }

    @XmlElement(name = "UserName")
    public String getUserName() {
        return super.getUserId();
    }

    @XmlElement(name = "TradeNo")
    public String getTradeNumber() {
        return super.getTradeNumber();
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
    public ExceptionTradeNarrative getNarrative() {
        return this.narrative;
    }
}