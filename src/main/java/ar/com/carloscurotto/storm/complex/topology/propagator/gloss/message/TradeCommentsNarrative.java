package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.Validate;

@XmlRootElement(name = "Narratives")
public class TradeCommentsNarrative {
    /**
     * Represents the code for the update messages internal comments.
     */
    public static final String INTERNAL_NARRATIVE_CODE = "SINT";
    
    private Integer numberOfNarratives;
    private String narrativeCode1;
    private String narrativeText1;
    
    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public TradeCommentsNarrative(){}
    
    /**
     * Constructs an instance with the given String, using it as narrative text and setting the number of
     * narratives to one.
     * 
     * @param theInternalComments
     *            a String with internal comments for the narrative. It can't be blank
     * 
     */
    public TradeCommentsNarrative(final String theInternalComments) {
        Validate.notBlank(theInternalComments, "internalComments cannot be blank");
        narrativeCode1 = INTERNAL_NARRATIVE_CODE;
        narrativeText1 = theInternalComments;
        numberOfNarratives = 1;
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
}