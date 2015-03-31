package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.junit.Assert.assertEquals;

import org.easymock.EasyMockSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.test.util.ReflectionTestUtils;

public class TradeCommentsNarrativeTest extends EasyMockSupport {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void constructorWithValidInternalComments() {
        TradeCommentsNarrative narrative = new TradeCommentsNarrative("internalComments");
        assertEquals(ReflectionTestUtils.getField(narrative, "INTERNAL_NARRATIVE_CODE"),
                narrative.getNarrativeCode1());
        assertEquals("internalComments", narrative.getNarrativeText1());
        assertEquals((Integer) 1, (Integer) narrative.getNoOfNarratives());
    }

    @Test
    public void constructorWithNullInternalComments() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("internalComments cannot be blank");
        new TradeCommentsNarrative(null);
    }
    
    @Test
    public void constructorWithEmptyInternalComments() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("internalComments cannot be blank");
        new TradeCommentsNarrative("");
    }
    
    @Test
    public void constructorWithBlankInternalComments() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("internalComments cannot be blank");
        new TradeCommentsNarrative(" ");
    }
}
