package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.Validate;

@XmlRootElement(name = "Narratives")
public class TradeCommentsNarrative {

    /**
     * Represents the code for the update messages internal comments.
     */
    private static final String INTERNAL_NARRATIVE_CODE = "SINT";

    private String narrativeText1;

    /**
     * This constructor should not be used. It is only here for the use of the JAXB framework that needs it for
     * unmarshalling process and to check annotation compliance to the framework standards
     */
    @Deprecated
    public TradeCommentsNarrative() {
    }

    /**
     * Constructs an instance with the given String, using it as narrative text and setting the number of narratives to
     * one.
     *
     * @param theInternalComments
     *            the comments for this narrative. It can't be blank
     *
     */
    public TradeCommentsNarrative(final String theInternalComments) {
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