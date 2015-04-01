package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

@RunWith(EasyMockRunner.class)
public class ExceptionTradeStatusMessageTest extends EasyMockSupport {
    
    @Mock
    private UpdateRow updateRowMock;
    
    @Test
    public void constructorShouldSucceed(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a userId");
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        replayAll();
        ExceptionTradeStatusMessage message = new ExceptionTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("a userId", message.getUserName());
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText1());
        assertEquals("a status code", message.getNarrative().getNarrativeText2());
    }
   
}

