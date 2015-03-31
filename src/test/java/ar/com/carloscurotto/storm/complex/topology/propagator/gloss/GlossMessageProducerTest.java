package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.GlossMessageMarshaller;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.GlossMessageProducer;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

@RunWith(EasyMockRunner.class)
public class GlossMessageProducerTest extends EasyMockSupport {
    @Mock
    private OpenAwareProducer<String> messageProducerMock;
    
    @Mock
    private GlossMessageMarshaller glossMessageMarshaller;
    
    @Mock
    private NormalTradeStatusMessage message;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void constructorWithValidProducerAndValidMarshaller(){
        GlossMessageProducer producer = new GlossMessageProducer(messageProducerMock, glossMessageMarshaller);
        assertEquals(messageProducerMock, ReflectionTestUtils.getField(producer, "messageProducer"));
        assertEquals(glossMessageMarshaller, ReflectionTestUtils.getField(producer, "messageMarshaller"));
    }
    
    @Test
    public void constructorWithValidProducerAndNullMarshaller(){
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message marshaller cannot be null");
        
        new GlossMessageProducer(messageProducerMock, null);
    }
    
    @Test
    public void constructorWithNullProducerAndValidMarshaller(){
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message producer cannot be null");
        
        new GlossMessageProducer(null, glossMessageMarshaller);
    }
    
    @Test
    public void sendWithNullMessages() {
        OpenAwareProducer<String> messageProducer = new TestMessageProducer();
        GlossMessageProducer producer = new GlossMessageProducer(messageProducer, glossMessageMarshaller);
       
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The messages cannot be null.");
        
        producer.open();
        producer.send(null);
    }
    
    @Test
    public void sendWithEmptyMessages() {
        OpenAwareProducer<String> messageProducer = new TestMessageProducer();
        GlossMessageProducer producer = new GlossMessageProducer(messageProducer, glossMessageMarshaller);
               
        producer.open();
        producer.send(new ArrayList<TradeMessage>());
    }
    
    @Test
    public void sendWithMessagesHavingNullAsMessage() {
        OpenAwareProducer<String> messageProducer = new TestMessageProducer();
        GlossMessageProducer producer = new GlossMessageProducer(messageProducer, glossMessageMarshaller);
       
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("The message cannot be null");
        
        producer.open();
        List<TradeMessage> messages = new ArrayList<TradeMessage>();
        messages.add(null);
        producer.send(messages);
    }
    
    @Test
    public void sendWithMessagesHavingAMessage() {
        OpenAwareProducer<String> messageProducer = new TestMessageProducer();
        
        GlossMessageProducer producer = new GlossMessageProducer(messageProducer, glossMessageMarshaller);
        producer.open();

        List<TradeMessage> messages = new ArrayList<TradeMessage>();
        messages.add(message);
        
        expect(glossMessageMarshaller.marshal(message)).andReturn("marshalled message");
        
        replayAll();
        producer.send(messages);
        verifyAll();
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
