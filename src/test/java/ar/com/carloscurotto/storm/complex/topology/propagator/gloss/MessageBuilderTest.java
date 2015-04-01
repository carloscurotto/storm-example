package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeCommentsMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

@RunWith(EasyMockRunner.class)
public class MessageBuilderTest extends EasyMockSupport {
    @Mock
    private UpdateRow updateRowMock;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void buildNormalTradeWithoutComments() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", true);
        theParameters.put("exceptionTrade", false);
        
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        
        theParameters.put("updateInternalComment", false);
        
        replayAll();
        List<TradeMessage> messages = new GlossMessageBuilder().build(theParameters, updateRowMock);
        verifyAll();
        
        assertNotNull(messages);
        assertEquals(1, messages.size());
        
        NormalTradeStatusMessage message = (NormalTradeStatusMessage)messages.get(0);
        assertNotNull(message);
    }
 
    @Test
    public void buildNormalTradeWithComments() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", true);
        theParameters.put("exceptionTrade", false);
        
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo").times(2);
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id").times(2);
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        
        theParameters.put("updateInternalComment", true);
        
        expect(updateRowMock.getUpdateColumnValue("internalComments")).andReturn("an internal comments");
        
        replayAll();
        List<TradeMessage> messages = new GlossMessageBuilder().build(theParameters, updateRowMock);
        verifyAll();
        
        assertNotNull(messages);
        assertEquals(2, messages.size());
        
        NormalTradeStatusMessage message = (NormalTradeStatusMessage)messages.get(0);
        assertNotNull(message);
        
        TradeCommentsMessage comment = (TradeCommentsMessage)messages.get(1);
        assertNotNull(comment);
    }
    
    @Test
    public void buildExceptionTradeWithoutComments() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", true);
        theParameters.put("exceptionTrade", true);
        
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a userId");
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        theParameters.put("updateInternalComment", false);
        
        replayAll();
        List<TradeMessage> messages = new GlossMessageBuilder().build(theParameters, updateRowMock);
        verifyAll();
        
        assertNotNull(messages);
        assertEquals(1, messages.size());
        
        ExceptionTradeStatusMessage message = (ExceptionTradeStatusMessage)messages.get(0);
        assertNotNull(message);        
    }
    
    @Test
    public void buildExceptionTradeWithComments() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", true);
        theParameters.put("exceptionTrade", true);
        
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo").times(2);
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a userId").times(2);
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        theParameters.put("updateInternalComment", true);
        
        expect(updateRowMock.getUpdateColumnValue("internalComments")).andReturn("an internal comments");
        
        replayAll();
        List<TradeMessage> messages = new GlossMessageBuilder().build(theParameters, updateRowMock);
        verifyAll();
                
        assertNotNull(messages);
        assertEquals(2, messages.size());
        
        ExceptionTradeStatusMessage message = (ExceptionTradeStatusMessage)messages.get(0);
        assertNotNull(message);
        
        TradeCommentsMessage comment = (TradeCommentsMessage)messages.get(1);
        assertNotNull(comment);
    }
    
    @Test
    public void buildWithNullParametersMap() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The parameters map cannot be null.");
        
        new GlossMessageBuilder().build(null, updateRowMock);
    }
    
    @Test
    public void buildWithNullUpdateRow() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The updateRow cannot be null.");
        
        new GlossMessageBuilder().build(new HashMap<String, Object>(), null);
    }
    
    @Test
    public void buildWithNullUpdateStatus() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", null);
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("updateStatus cannot be null.");
        
        new GlossMessageBuilder().build(theParameters, updateRowMock);
    }
    
    @Test
    public void buildWithNullUpdateInternalComment() {
        Map<String, Object> theParameters = new HashMap<String, Object>();
        theParameters.put("updateStatus", true);
        theParameters.put("exceptionTrade", null);
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("exceptionTrade cannot be null.");
        
        new GlossMessageBuilder().build(theParameters, updateRowMock);
    }
}
