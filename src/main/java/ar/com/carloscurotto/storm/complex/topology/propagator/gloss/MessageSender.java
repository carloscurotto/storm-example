package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception.GlossJmsException;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.exception.GlossMarshalException;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExcpTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.NormalTradeStatusMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeCommentsMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

import ar.com.carloscurotto.storm.complex.transport.Producer;

/**
 * Sends the message into the queue. Knows what queue to use and knows how to marshall the messages.
 *
 * @author D540601
 */
public class MessageSender extends OpenAwareService<Messages, Void> {

    protected Map<Class<? extends TradeMessage>, Marshaller> marshallers = new HashMap<Class<? extends TradeMessage>, Marshaller>();

    private Producer<String> messageProducer;

    /**
     * Constructs the MessageSender with the give message producer.
     * 
     * @param theMessageProducer
     *            a {@link Producer<String>} that will be used to send the update messages to the consumer of the
     *            producer.
     */
    public MessageSender(final Producer<String> theMessageProducer) {
        Validate.notNull(theMessageProducer, "The message producer cannot be null");
        messageProducer = theMessageProducer;
    }

    @Override
    protected void doOpen() {
        try {
            marshallers.put(NormalTradeStatusMessage.class,
                    JAXBContext.newInstance(NormalTradeStatusMessage.class).createMarshaller());
            marshallers.put(ExcpTradeStatusMessage.class,
                    JAXBContext.newInstance(ExcpTradeStatusMessage.class).createMarshaller());
            marshallers.put(TradeCommentsMessage.class,
                    JAXBContext.newInstance(TradeCommentsMessage.class).createMarshaller());
        } catch (JAXBException e) {
            throw new RuntimeException(
                    "MessageSender can't create the marshallers needed to convert the trade objects to xml",
                    e);
        }

    }

    @Override
    protected void doClose() {
        messageProducer.close();
    }

    @Override
    protected Void doExecute(Messages theMessages) {
        send(theMessages.getMainMessage());
        if (theMessages.getCommentMessage() != null) {
            send(theMessages.getCommentMessage());
        }
        return null;
    }

    /**
     * Sends a message into the queue after marshalling into a String.
     *
     * @param theMessage
     *            a TradeMessage instance. It cannot be null.
     * @throws GlossJmsException
     *             when the message cannot be sent.
     */
    public void send(TradeMessage theMessage) {
        Validate.notNull(theMessage, "message cannot be null");
        messageProducer.send(marshal(theMessage));
    }

    /**
     * Returns a string representing the message parameter.
     *
     * @param theMessage
     *            the message to marshal to a string. It cannot be null.
     * @return the marshalled string representation of the message parameter.
     */
    protected String marshal(TradeMessage theMessage) {
        Validate.notNull(theMessage, "message cannot be null");

        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);

        Marshaller marshaller = marshallers.get(theMessage.getClass());

        try {
            marshaller.marshal(theMessage, result);
            return writer.toString();
        } catch (JAXBException e) {
            throw new GlossMarshalException(e);
        }
    }
}