package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import org.apache.commons.lang3.Validate;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
/**
 * Internal element for messages
 * @author D540601
 *
 */
@XmlRootElement(name = "Narrative")
public class Narrative {

    public static final String INTERNAL_NARRATIVE_CODE = "SINT";
    public static final String EXTERNAL_NARRATIVE_CODE = "ERR";

    private String narrativeCode;
    private String narrativeText;

    /**
     * Default constructor.
     */
    public Narrative() {}
    
    /**
     * Constructor.
     *
     * @param code
     *            a String.
     * @param text
     *            a String.
     * @throws IllegalArgumentException
     *             if either code or tet is blank
     */
    public Narrative(String code, String text) {
        Validate.notBlank(code, "code cannot be blank");
        Validate.notBlank(text, "text cannt be blank");
        this.narrativeCode = code;
        this.narrativeText = text;
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