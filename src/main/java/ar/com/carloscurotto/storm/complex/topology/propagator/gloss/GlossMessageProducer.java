package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.util.List;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

/**
 * Sends the message into the queue. Knows what queue to use and knows how to marshall the messages.
 *
 * @author D540601
 */
public class GlossMessageProducer extends OpenAwareProducer<List<GlossMessage>> {

    private OpenAwareProducer<String> messageProducer;
    private GlossMessageMarshaller messageMarshaller;

    /**
     * Constructs the GlossMessageProducer with the given message producer and a message marshaller.
     *
     * @param theMessageProducer
     *            a {@link Producer<String>} that will be used to send messages to the transport layer. It cannot be
     *            null.
     * @param theMessageMarshaller
     *            the marshaller used for creating the xml that will be send to the transport layer. It cannot be null.
     */
    public GlossMessageProducer(final OpenAwareProducer<String> theMessageProducer,
            final GlossMessageMarshaller theMessageMarshaller) {
        Validate.notNull(theMessageProducer, "The message producer cannot be null");
        Validate.notNull(theMessageMarshaller, "The message marshaller cannot be null");
        messageProducer = theMessageProducer;
        messageMarshaller = theMessageMarshaller;
    }

    @Override
    protected void doOpen() {
        messageProducer.open();
    }

    @Override
    protected void doClose() {
        messageProducer.close();
    }

    /**
     * Sends the given message as XML to the internal consumer.
     *
     * @param theMessages
     *            a {@link List<GlossMessage>} with the messages to be sent. It cannot be null.
     */
    @Override
    protected void doSend(List<GlossMessage> theMessages) {
        Validate.notNull(theMessages, "The messages cannot be null.");
        for (GlossMessage message : theMessages) {
            send(message);
        }
    }

    private void send(GlossMessage theMessage) {
        messageProducer.send(messageMarshaller.marshal(theMessage));
    }
}