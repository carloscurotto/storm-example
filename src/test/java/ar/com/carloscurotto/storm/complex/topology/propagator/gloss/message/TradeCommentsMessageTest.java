package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

@RunWith(EasyMockRunner.class)
public class TradeCommentsMessageTest extends EasyMockSupport {

    @Mock
    private UpdateRow updateRowMock;

    @Test
    public void contructorWithValidInternalComments() {
        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        expect(updateRowMock.getUpdateColumnValue("internalComments")).andReturn("an internal comments");

        replayAll();
        TradeCommentsMessage message = new TradeCommentsMessage(updateRowMock);
        verifyAll();
        
        assertEquals(ReflectionTestUtils.getField(message, "DEFAULT_EVENT_CODE"), message.getEventCode());
        assertEquals(ReflectionTestUtils.getField(message, "DEFAULT_EVENT_TYPE"), message.getEventType());
        assertNotNull(message.getNarrative());
    }
}
