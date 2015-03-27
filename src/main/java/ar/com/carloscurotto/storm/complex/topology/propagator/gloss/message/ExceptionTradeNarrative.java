package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;

/**
 * Internal element for trade messages. It is used mostly to represent internal and external comments on an exception
 * trade update message. It is intended to be marshalled into XML string using the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "Narratives")
public class ExceptionTradeNarrative {

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

    /**
     * This constructor should not be used.
     * It is only here for the use of the JAXB framework that needs it for unmarshalling process
     * and to check annotation compliance to the framework standards
     */
    @Deprecated
    public ExceptionTradeNarrative() {
    }
        
    /**
     * Constructs an ExceptionTradeNarrative with "theExternalComments" as narrativeText1 or
     * theStatusCode as narrativeText1 or narrativeText2 depending on the current case.
     * @param theExternalComments text used as external comments. It can be null or blank.
     * @param theStatusCode text used as status code. It can be null or blank.
     */
    public ExceptionTradeNarrative(final String theExternalComments, final String theStatusCode) {        
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