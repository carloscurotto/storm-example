package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Internal element for trade messages. It is used mostly to represent internal and external comments on an exception
 * trade update message. It is intended to be marshalled into XML string using the JAXB framework.
 * 
 * @author D540601
 *
 */
@XmlRootElement(name = "Narratives")
public class ExceptionTradeNarrative {

    private Integer noOfNarratives = 0;
    private String narrativeCode1;
    private String narrativeText1;
    private String narrativeCode2;
    private String narrativeText2;
    private String narrativeCode3;
    private String narrativeText3;

    public ExceptionTradeNarrative() {
    }

    @XmlElement(name = "NoOfNarratives")
    public Integer getNoOfNarratives() {
        return this.noOfNarratives;
    }

    public void setNoOfNarratives(Integer noOfNarratives) {
        this.noOfNarratives = noOfNarratives;
    }

    @XmlElement(name = "NarrativeCode1")
    public String getNarrativeCode1() {
        return this.narrativeCode1;
    }

    public void setNarrativeCode1(String narrativeCode1) {
        this.narrativeCode1 = narrativeCode1;
    }

    @XmlElement(name = "NarrativeText1")
    public String getNarrativeText1() {
        return this.narrativeText1;
    }

    public void setNarrativeText1(String narrativeText1) {
        this.narrativeText1 = narrativeText1;
    }

    @XmlElement(name = "NarrativeCode2")
    public String getNarrativeCode2() {
        return this.narrativeCode2;
    }

    public void setNarrativeCode2(String narrativeCode2) {
        this.narrativeCode2 = narrativeCode2;
    }

    @XmlElement(name = "NarrativeText2")
    public String getNarrativeText2() {
        return this.narrativeText2;
    }

    public void setNarrativeText2(String narrativeText2) {
        this.narrativeText2 = narrativeText2;
    }

    @XmlElement(name = "NarrativeCode3")
    public String getNarrativeCode3() {
        return narrativeCode3;
    }

    public void setNarrativeCode3(String narrativeCode3) {
        this.narrativeCode3 = narrativeCode3;
    }

    @XmlElement(name = "NarrativeText3")
    public String getNarrativeText3() {
        return narrativeText3;
    }

    public void setNarrativeText3(String narrativeText3) {
        this.narrativeText3 = narrativeText3;
    }
}