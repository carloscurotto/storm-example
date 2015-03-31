package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;


public class NormalTradeNarrativeTest {
    private NormalTradeNarrative narrative;
    
    @Test
    public void constructorWithExternalComments(){
        narrative = new NormalTradeNarrative("external comments");
        String code = (String)ReflectionTestUtils.getField(narrative, "EXTERNAL_NARRATIVE_CODE");
        
        assertEquals(code, narrative.getNarrativeCode());
        assertEquals("external comments", narrative.getNarrativeText());
    }
    
    @Test
    public void constructorWithNullExternalComments(){
        narrative = new NormalTradeNarrative(null);
        String code = (String)ReflectionTestUtils.getField(narrative, "EXTERNAL_NARRATIVE_CODE");
        
        assertEquals(code, narrative.getNarrativeCode());
        assertEquals(null, narrative.getNarrativeText());
    }
    
    @Test
    public void constructorWithEmptyExternalComments(){
        narrative = new NormalTradeNarrative("");
        String code = (String)ReflectionTestUtils.getField(narrative, "EXTERNAL_NARRATIVE_CODE");
        
        assertEquals(code, narrative.getNarrativeCode());
        assertEquals("", narrative.getNarrativeText());
    }
    
    @Test
    public void constructorWithBlankExternalComments(){
        narrative = new NormalTradeNarrative(" ");
        String code = (String)ReflectionTestUtils.getField(narrative, "EXTERNAL_NARRATIVE_CODE");
        
        assertEquals(code, narrative.getNarrativeCode());
        assertEquals(" ", narrative.getNarrativeText());
    }
}