package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

@RunWith(EasyMockRunner.class)
public class TradeMessageTest extends EasyMockSupport {
    @Mock
    private UpdateRow updateRowMock;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidUpdateRowAndNullMessageType() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The messageType cannot be blank.");
        new TradeMessage(updateRowMock, null){};
    }
    
    @Test
    public void constructorWithValidUpdateRowAndEmptyMessageType() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The messageType cannot be blank.");
        new TradeMessage(updateRowMock, ""){};
    }
    
    @Test
    public void constructorWithValidUpdateRowAndBlankMessageType() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The messageType cannot be blank.");
        new TradeMessage(updateRowMock, " "){};
    }
    
    
    @Test
    public void constructorWithUpdateRowNullAndValidMessageType() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The update row cannot be null.");
        new TradeMessage(null, "messageType"){};
    }
    
    @Test
    public void constructorTradeNumberInUpdateRowIsNullAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn(null);
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a user id");
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The trade number cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorTradeNumberInUpdateRowIsEmptyAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a user id");
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The trade number cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorTradeNumberInUpdateRowIsBlankAndValidMessageType() {
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn(" ");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a user id");
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The trade number cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorUserIdInUpdateRowIsNullAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn(null);
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The userId cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorUserIdInUpdateRowIsBlankAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn(" ");
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The userId cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorUserIdInUpdateRowIsEmptyAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("");
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The userId cannot be blank");
        
        replayAll();
        new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
    }
    
    @Test
    public void constructorWithValidUserIdAndValidTradeNoInUpdateRowAndValidMessageType(){
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("a userId");
        
        replayAll();
        TradeMessage message = new TradeMessage(updateRowMock, "a message type"){};
        verifyAll();
        
        assertEquals("a tradeNo", message.getTradeNumber());
        assertEquals("a userId", message.getUserId());
        assertEquals("a message type", message.getMsgType());
        
    }
}
