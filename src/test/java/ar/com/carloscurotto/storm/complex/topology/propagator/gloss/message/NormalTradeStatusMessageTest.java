package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

@RunWith(EasyMockRunner.class)
public class NormalTradeStatusMessageTest extends EasyMockSupport {
    
    @Mock
    private UpdateRow updateRowMock;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidExternalCommentsAndValidServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
               
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals("a service name", message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText());
    }
    
    @Test
    public void constructorWithNullExternalCommentsAndValidServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals("a service name", message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText());
    }
    
    @Test
    public void constructorWithEmptyExternalCommentsAndValidServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals("a service name", message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNull(message.getNarrative());
    }
    
    @Test
    public void constructorWithBlankExternalCommentsAndValidServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn(" ");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals("a service name", message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNull(message.getNarrative());
    }
    
    @Test
    public void constructorWithValidExternalCommentsAndNullServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn(null);
               
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals(ReflectionTestUtils.getField(message, "DEFAULT_SERVICE_NAME"), message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText());
    }
    
    @Test
    public void constructorWithValidExternalCommentsAndEmptyServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("");
               
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals(ReflectionTestUtils.getField(message, "DEFAULT_SERVICE_NAME"), message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText());
    }
    
    @Test
    public void constructorWithValidExternalCommentsAndBlankServiceName(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn(" ");
               
        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("an user id", message.getUserId());
        assertEquals("1", message.getInstNumber());
        assertEquals("a status code", message.getStatusCode());
        assertEquals(ReflectionTestUtils.getField(message, "DEFAULT_SERVICE_NAME"), message.getServiceName());
        assertNotNull(message.getStatusDate());
        
        assertNotNull(message.getNarrative());
        assertEquals("an external comment", message.getNarrative().getNarrativeText());
    }
    
    @Test
    public void constructorWithNullInstNumber(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(null);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service");
               
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The instNumber cannot be null");
        
        replayAll();
        new NormalTradeStatusMessage(updateRowMock);
    }
    
    @Test
    public void constructorWithNullStatusCode(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn(null);
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service");
               
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The statusCode cannot be blank");
        
        replayAll();
        new NormalTradeStatusMessage(updateRowMock);
    }
    
    @Test
    public void constructorWithEmptyStatusCode(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service");
               
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The statusCode cannot be blank");
        
        replayAll();
        new NormalTradeStatusMessage(updateRowMock);
    }
    
    @Test
    public void constructorWithBlankStatusCode(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn("an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn(" ");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service");
               
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The statusCode cannot be blank");
        
        replayAll();
        new NormalTradeStatusMessage(updateRowMock);
    }
}
