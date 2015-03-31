package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(EasyMockRunner.class)
public class ExceptionTradeNarrativeTest {
    private ExceptionTradeNarrative narrative;
        
    @Test
    public void constructorWithExternalCommentsNullAndStatusCodeNull() {
        narrative = new ExceptionTradeNarrative(null, null);
        assertEquals((Integer)0, narrative.getNoOfNarratives());
        assertNull(narrative.getNarrativeCode1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsNullAndStatusCodeBlank() {
        narrative = new ExceptionTradeNarrative(null, "");
        assertEquals((Integer)0, narrative.getNoOfNarratives());
        assertNull(narrative.getNarrativeCode1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsBlankAndStatusCodeNull() {
        narrative = new ExceptionTradeNarrative(" ", null);
        assertEquals((Integer)0, narrative.getNoOfNarratives());
        assertNull(narrative.getNarrativeCode1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsBlankAndStatusCodeBlank() {
        narrative = new ExceptionTradeNarrative(" ", null);
        assertEquals((Integer)0, narrative.getNoOfNarratives());
        assertNull(narrative.getNarrativeCode1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsAndStatusCodeNull() {
        narrative = new ExceptionTradeNarrative("theExternalComment", null);
        assertEquals((Integer)1, narrative.getNoOfNarratives());
        String narrativeCode = (String)ReflectionTestUtils.getField(narrative, "SCLT_NARRATIVE_CODE");
        assertEquals(narrativeCode, narrative.getNarrativeCode1());
        assertEquals("theExternalComment", narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsAndStatusCodeBlank() {
        narrative = new ExceptionTradeNarrative("theExternalComment", "");
        assertEquals((Integer)1, narrative.getNoOfNarratives());
        String narrativeCode = (String)ReflectionTestUtils.getField(narrative, "SCLT_NARRATIVE_CODE");
        assertEquals(narrativeCode, narrative.getNarrativeCode1());
        assertEquals("theExternalComment", narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsNullAndStatusCode() {
        narrative = new ExceptionTradeNarrative(null, "theStatusCode");
        assertEquals((Integer)1, narrative.getNoOfNarratives());
        String narrativeCode = (String)ReflectionTestUtils.getField(narrative, "ISTS_NARRATIVE_CODE");
        assertEquals(narrativeCode, narrative.getNarrativeCode1());
        assertEquals("theStatusCode", narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsBlankAndStatusCode() {
        narrative = new ExceptionTradeNarrative(null, "theStatusCode");
        assertEquals((Integer)1, narrative.getNoOfNarratives());
        String narrativeCode = (String)ReflectionTestUtils.getField(narrative, "ISTS_NARRATIVE_CODE");
        assertEquals(narrativeCode, narrative.getNarrativeCode1());
        assertEquals("theStatusCode", narrative.getNarrativeText1());
        assertNull(narrative.getNarrativeCode2());
        assertNull(narrative.getNarrativeText2());
    }
    
    @Test
    public void constructorWithExternalCommentsAndStatusCode() {
        narrative = new ExceptionTradeNarrative("theExternalComments", "theStatusCode");
        assertEquals((Integer)2, narrative.getNoOfNarratives());
        String narrativeCode1 = (String)ReflectionTestUtils.getField(narrative, "SCLT_NARRATIVE_CODE");
        assertEquals(narrativeCode1, narrative.getNarrativeCode1());
        assertEquals("theExternalComments", narrative.getNarrativeText1());
        
        String narrativeCode2 = (String)ReflectionTestUtils.getField(narrative, "ISTS_NARRATIVE_CODE");
        assertEquals(narrativeCode2, narrative.getNarrativeCode2());
        assertEquals("theStatusCode", narrative.getNarrativeText2());
    }
}
