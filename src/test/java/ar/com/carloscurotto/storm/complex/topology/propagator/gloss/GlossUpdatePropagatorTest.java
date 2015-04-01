package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.ResultStatus;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

@RunWith(EasyMockRunner.class)
public class GlossUpdatePropagatorTest extends EasyMockSupport {
    @Mock
    private UpdateRow updateRowMock;
    
    @Mock
    private GlossMessageProducer glossMessageProducerMock;
    
    @Mock
    private GlossMessageBuilder messageBuilderMock;
    
    @Mock
    private UpdatePropagatorContext updatePropagatorContext;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidSenderAndValidBuilder() {
        GlossUpdatePropagator propagator = new GlossUpdatePropagator(glossMessageProducerMock, messageBuilderMock);
        assertEquals(glossMessageProducerMock, ReflectionTestUtils.getField(propagator, "messageSender"));
        assertEquals(messageBuilderMock, ReflectionTestUtils.getField(propagator, "messageBuilder"));
    }
    
    @Test
    public void constructorWithValidSenderAndNullBuilder(){
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message builder can not be null.");
        
        new GlossUpdatePropagator(glossMessageProducerMock, null);
    }
    
    @Test
    public void constructorWithNullSenderAndValidBuilder() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message sender can not be null.");
        
        new GlossUpdatePropagator(null, messageBuilderMock);
    }
    
    @Test
    public void doPropagateWithValidContext() {
        Map<String, Object> parameters = new HashMap<String, Object>();
        expect(updatePropagatorContext.getParameters()).andReturn(parameters);
        expect(updatePropagatorContext.getRow()).andReturn(updateRowMock);
        
        List<TradeMessage> messages = new ArrayList<TradeMessage>();
        expect(messageBuilderMock.build(parameters, updateRowMock)).andReturn(messages);
        
        List<Class<? extends TradeMessage>> messageClasses = new ArrayList<Class<? extends TradeMessage>>();
        messageClasses.add(NormalTradeStatusMessage.class);
        
        TestGlossMessageProducer testMessageProducer = new TestGlossMessageProducer(new TestMessageProducer(), new GlossMessageMarshaller(messageClasses));
        GlossUpdatePropagator propagator = new GlossUpdatePropagator(testMessageProducer, messageBuilderMock);
        
        replayAll();
        testMessageProducer.open();
        UpdatePropagatorResult result = propagator.doPropagate(updatePropagatorContext);
        verifyAll();
        
        assertEquals(ResultStatus.SUCCESS, result.getStatus());
        assertEquals("SUCCESS", result.getMessage());
    }
    
    @Test
    public void doPropagateWithNullContext(){
        List<Class<? extends TradeMessage>> messageClasses = new ArrayList<Class<? extends TradeMessage>>();
        messageClasses.add(NormalTradeStatusMessage.class);
        
        TestGlossMessageProducer testMessageProducer = new TestGlossMessageProducer(new TestMessageProducer(), new GlossMessageMarshaller(messageClasses));
        GlossUpdatePropagator propagator = new GlossUpdatePropagator(testMessageProducer, messageBuilderMock);
        
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The update cannot be null.");
        
        replayAll();
        testMessageProducer.open();
        propagator.doPropagate(null);
        verifyAll();
    }
    
    @Test
    public void doPropagateWithNullParameters() {
        expect(updatePropagatorContext.getParameters()).andReturn(null);
        expect(updatePropagatorContext.getRow()).andReturn(updateRowMock);
                
        List<Class<? extends TradeMessage>> messageClasses = new ArrayList<Class<? extends TradeMessage>>();
        messageClasses.add(NormalTradeStatusMessage.class);
        
        TestGlossMessageProducer testMessageProducer = new TestGlossMessageProducer(new TestMessageProducer(), new GlossMessageMarshaller(messageClasses));
        GlossUpdatePropagator propagator = new GlossUpdatePropagator(testMessageProducer, messageBuilderMock);
        
        replayAll();
        testMessageProducer.open();
        UpdatePropagatorResult result = propagator.doPropagate(updatePropagatorContext);
        verifyAll();
        
        assertEquals(ResultStatus.FAILURE, result.getStatus());
        assertEquals("theParameters cannot be null.", result.getMessage());
    }
    
    @Test
    public void doPropagateWithNullUpdateRow() {
        expect(updatePropagatorContext.getParameters()).andReturn(new HashMap<String, Object>());
        expect(updatePropagatorContext.getRow()).andReturn(null);
                
        List<Class<? extends TradeMessage>> messageClasses = new ArrayList<Class<? extends TradeMessage>>();
        messageClasses.add(NormalTradeStatusMessage.class);
        
        TestGlossMessageProducer testMessageProducer = new TestGlossMessageProducer(new TestMessageProducer(), new GlossMessageMarshaller(messageClasses));
        GlossUpdatePropagator propagator = new GlossUpdatePropagator(testMessageProducer, messageBuilderMock);
        
        replayAll();
        testMessageProducer.open();
        UpdatePropagatorResult result = propagator.doPropagate(updatePropagatorContext);
        verifyAll();
        
        assertEquals(ResultStatus.FAILURE, result.getStatus());
        assertEquals("theUpdateRow cannot be null.", result.getMessage());
    }
    
    /**
     * Class that inherits from OpenAwareProducer to be used as a producer for unit testing
     * @author D540601
     */
    private static class TestGlossMessageProducer extends GlossMessageProducer {
        public TestGlossMessageProducer(OpenAwareProducer<String> theMessageProducer,
                GlossMessageMarshaller theMessageMarshaller) {
            super(theMessageProducer, theMessageMarshaller);
        }
    }
    
    /**
     * Class that inherits from OpenAwareProducer to be used as a producer for unit testing
     * @author D540601
     */
    private static class TestMessageProducer extends OpenAwareProducer<String> {

        @Override
        protected void doSend(String theContext) {
            // no implementation needed
        }

        @Override
        protected void doOpen() {
            // no implementation needed
        }

        @Override
        protected void doClose() {
            // no implementation needed
        }
    }
}
