package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeCommentsMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

@RunWith(EasyMockRunner.class)
public class GlossMessageMarshallerTest extends EasyMockSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private UpdateRow updateRowMock;
    
    @Mock
    private Marshaller normalTradeMarshallerMock;

    private List<Class<? extends TradeMessage>> classes;

    @Before
    public void setUp() {
        classes = new ArrayList<Class<? extends TradeMessage>>();
    }

    @Test
    public void constructorWithMessageClasses() {
        classes.add(ExceptionTradeStatusMessage.class);
        classes.add(NormalTradeStatusMessage.class);
        classes.add(TradeCommentsMessage.class);

        GlossMessageMarshaller glossMessageMarshaller = new GlossMessageMarshaller(classes);

        List<Class<? extends TradeMessage>> messageClasses = (List<Class<? extends TradeMessage>>) ReflectionTestUtils
                .getField(glossMessageMarshaller, "messageClasses");
        assertNotNull(messageClasses);
        assertEquals(classes.size(), messageClasses.size());
        assertTrue(messageClasses.contains(ExceptionTradeStatusMessage.class));
        assertTrue(messageClasses.contains(NormalTradeStatusMessage.class));
        assertTrue(messageClasses.contains(TradeCommentsMessage.class));

        Map<Class<? extends TradeMessage>, Marshaller> marshallers = (Map<Class<? extends TradeMessage>, Marshaller>) ReflectionTestUtils
                .getField(glossMessageMarshaller, "marshallers");
        assertNotNull(marshallers);
        assertNotNull(marshallers.get(ExceptionTradeStatusMessage.class));
        assertNotNull(marshallers.get(NormalTradeStatusMessage.class));
        assertNotNull(marshallers.get(TradeCommentsMessage.class));
    }

    @Test
    public void constructorWithEmptyMessageClasses() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The message classes list cannot be empty");

        new GlossMessageMarshaller(classes);
    }

    @Test
    public void constructorWithNullMessageClasses() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message classes list cannot be empty");

        new GlossMessageMarshaller(null);
    }

    @Test
    public void marshalWithValidTradeMessage() {
        classes.add(NormalTradeStatusMessage.class);
        GlossMessageMarshaller glossMessageMarshaller = new GlossMessageMarshaller(classes);

        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn(
                "an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");

        replayAll();
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRowMock);
        String messageStr = glossMessageMarshaller.marshal(message);
        verifyAll();
        assertNotNull(messageStr);
        assertFalse(messageStr.isEmpty());
    }

    @Test
    public void marshalWithNullMessage() {
        classes.add(NormalTradeStatusMessage.class);
        GlossMessageMarshaller glossMessageMarshaller = new GlossMessageMarshaller(classes);

        thrown.expect(NullPointerException.class);
        thrown.expectMessage("the message cannot be null");

        glossMessageMarshaller.marshal(null);
    }

    @Test
    public void marshalWithNotConfiguredClass() {
        classes.add(ExceptionTradeStatusMessage.class);
        GlossMessageMarshaller glossMessageMarshaller = new GlossMessageMarshaller(classes);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Configuration error: there isn't any marshalled configured for class:"
                + NormalTradeStatusMessage.class);

        expect(updateRowMock.getUpdateColumnValue("tradeNo")).andReturn("a tradeNo");
        expect(updateRowMock.getUpdateColumnValue("userId")).andReturn("an user id");
        expect(updateRowMock.getUpdateColumnValue("externalComments")).andReturn(
                "an external comment");
        expect(updateRowMock.getUpdateColumnValue("instNumber")).andReturn(1L);
        expect(updateRowMock.getUpdateColumnValue("statusCode")).andReturn("a status code");
        expect(updateRowMock.getUpdateColumnValue("service")).andReturn("a service name");
        replayAll();
        glossMessageMarshaller.marshal(new NormalTradeStatusMessage(updateRowMock));
        verifyAll();
    }
    
    @Test
    public void marshalFailsWhenMarshallerFails() throws JAXBException {
        classes.add(ExceptionTradeStatusMessage.class);
        GlossMessageMarshaller glossMessageMarshaller = new GlossMessageMarshaller(classes);

        Map<Class<? extends TradeMessage>, Marshaller> marshallers = 
            new HashMap<Class<? extends TradeMessage>, Marshaller>();
        
        marshallers.put(NormalTradeStatusMessage.class, normalTradeMarshallerMock);
        ReflectionTestUtils.setField(glossMessageMarshaller, "marshallers", marshallers);
        
        thrown.expect(RuntimeException.class);
        thrown.expectCause(IsInstanceOf.<JAXBException>instanceOf(JAXBException.class));
        
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("tradeNo", "a tradeNo");
        updateColumns.put("userId", "an userId");
        updateColumns.put("externalComments", "an external comment");
        updateColumns.put("instNumber", 1l);
        updateColumns.put("statusCode", "a status code");
        updateColumns.put("service", "a service name");
        
        UpdateRow updateRow = new UpdateRow("id", new HashMap<String, Object>(), updateColumns);
        
        NormalTradeStatusMessage message = new NormalTradeStatusMessage(updateRow);
        
        normalTradeMarshallerMock.marshal(isA(NormalTradeStatusMessage.class), isA(StreamResult.class));
        expectLastCall().andThrow(new JAXBException("jaxbException"));

        replayAll();
        glossMessageMarshaller.marshal(message);
        verifyAll();
    }
}
