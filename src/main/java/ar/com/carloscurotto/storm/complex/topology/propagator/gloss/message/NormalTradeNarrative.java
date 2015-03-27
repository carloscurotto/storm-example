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
     * Represents the code for the update messages' internal comments.
     */
    public static final String INTERNAL_NARRATIVE_CODE = "SINT";

    /**
     * Represents the code for the update messages' external comments.
     */
    public static final String EXTERNAL_NARRATIVE_CODE = "ERR";

    /**
     * Represents the comment code for this narrative.
     */
    private String narrativeCode;

    /**
     * Represent the comment text for this narrative.
     */
    private String narrativeText;

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

    public void setNarrativeCode(String narrativeCode) {
        this.narrativeCode = narrativeCode;
    }

    @XmlElement(name = "narrativeText")
    public String getNarrativeText() {
        return narrativeText;
    }

    public void setNarrativeText(String narrativeText) {
        this.narrativeText = narrativeText;
    }
}