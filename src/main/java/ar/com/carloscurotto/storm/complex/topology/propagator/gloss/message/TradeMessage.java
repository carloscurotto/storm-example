package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

/**
 * Abstract parent class to all message classes on this package. Its main purpose is give a common type to all message
 * classes and ease the use of multiple types in maps and lists of messages.
 *
 * @author D540601
 */
public abstract class TradeMessage {
    /**
     * Default value for needReply xml attribute in gloss messages.
     */
    private static int DEFAULT_NEED_REPLY = 0;
    
    private String tradeNumber;
    private String messageType;
    private String userId;
    
    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public TradeMessage(){}
    
    /**
     * Constructor meant to be used by subclasses to initialize private members.
     * @param theUpdateRow {@link UpdateRow} with the needed column values to construct this TradeMessage. 
     * It must not be null and must contain tradeNo and userId.
     * @param theMessageType a String with the message type that this instance will represent. It can't be null.
     */
    protected TradeMessage(final UpdateRow theUpdateRow, final String theMessageType) {
        Validate.notNull(theUpdateRow, "The update row cannot be null.");
        Validate.notBlank(theMessageType, "The messageType cannot be blank.");
        
        String theTradeNumber = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");
        
        Validate.notBlank(theTradeNumber, "The trade number cannot be blank");
        Validate.notBlank(theUserId, "The userId cannot be blank");

        tradeNumber = theTradeNumber;
        messageType = theMessageType;
        userId = theUserId;
    }

    @XmlElement(name = "needReply")
    public Integer getNeedReply() {
        return DEFAULT_NEED_REPLY;
    }

    @XmlElement(name = "msgType")
    public String getMsgType() {
        return messageType;
    }

    public String getTradeNumber() {
        return tradeNumber;
    }
    
    protected String getUserId(){
        return userId;
    }
}
