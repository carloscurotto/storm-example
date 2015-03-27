package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Internal element for trade messages. It is used mostly to represent internal and external comments on an normal trade
 * update message. It is intended to be marshalled into XML string using the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "Narrative")
public class NormalTradeNarrative {

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

    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public NormalTradeNarrative(){}
    
    /**
     * Constructs the NormalTradeNarrative with the given external comments.
     * 
     * @param externalComments
     *            a String with the external comment. It doesn't require to have content. It can be null or blank.
     */
    public NormalTradeNarrative(final String externalComments) {
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