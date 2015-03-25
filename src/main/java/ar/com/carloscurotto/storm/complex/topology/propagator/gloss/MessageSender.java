package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;
import ar.com.carloscurotto.storm.complex.transport.Producer;

/**
 * Sends the message into the queue. Knows what queue to use and knows how to marshall the messages.
 *
 * @author D540601
 */
public class MessageSender extends OpenAwareService<List<TradeMessage>, Void> {

    private Map<Class<? extends TradeMessage>, Marshaller> marshallers = new HashMap<Class<? extends TradeMessage>, Marshaller>();
    private Producer<String> messageProducer;
    private List<Class<? extends TradeMessage>> messageClasses;

    /**
     * Constructs the MessageSender with the give message producer and the message classes.
     * 
     * @param theMessageProducer
     *            a {@link Producer<String>} that will be used to send the update messages to the consumer of the
     *            producer.
     * @param theMessageClasses
     *            a {@link List<Class<? extends TradeMessage>>} with the classes (.class) whose data this message sender
     *            will send.
     */
    public MessageSender(final Producer<String> theMessageProducer,
            List<Class<? extends TradeMessage>> theMessageClasses) {
        Validate.notNull(theMessageProducer, "The message producer cannot be null");
        Validate.notEmpty(theMessageClasses, "The message classes list cannot be empty");
        messageProducer = theMessageProducer;
        messageClasses = theMessageClasses;
    }

    /**
     * Creates the marshallers for the trade status messages classes and stores them in the marshallers map.
     * 
     * @throws {@link RuntimeException} when a JAXBContext or a marshaller can't be created for a "clazz".
     */
    @Override
    protected void doOpen() {
        messageProducer.open();

        try {
            for (Class<? extends TradeMessage> clazz : messageClasses) {
                marshallers.put(clazz, JAXBContext.newInstance(clazz).createMarshaller());
            }
        } catch (JAXBException e) {
            throw new RuntimeException(
                 "MessageSender can't create the marshallers needed to convert the trade objects to xml", e);
        }
    }

    @Override
    protected void doClose() {
        messageProducer.close();
    }

    /**
     * Sends the messages in theMessages parameters using the internal producer.
     * 
     * @param theMessages
     *            a {@link List<TradeMessage>} with the messages to be sent.
     * @return null.
     */
    @Override
    protected Void doExecute(List<TradeMessage> theMessages) {
        for (TradeMessage message : theMessages) {
            send(message);
        }
        return null;
    }

    /**
     * Sends a message after marshalling it into an xml string.
     *
     * @param theMessage
     *            a TradeMessage instance. It cannot be null.
     */
    private void send(TradeMessage theMessage) {
        Validate.notNull(theMessage, "The message cannot be null");
        messageProducer.send(marshal(theMessage));
    }

    /**
     * Returns a string representing the message parameter.
     *
     * @param theMessage
     *            the message to marshal to a string. It cannot be null.
     * @return the marshalled string representation of the message parameter.
     * @throws {@link RuntimeException} when there is not a marshaller configured for theMessage.class.
     * @throws {@link RuntimeException} when theMessage class marshaller can't marshal theMessage into a
     *         {@link StreamResult}.
     * @throws {@link RuntimeException} when the {@link StringWriter} used to retrieve the marshalled string can't close
     *         properly.
     */
    private String marshal(TradeMessage theMessage) {
        Validate.notNull(theMessage, "message cannot be null");

        String result = null;
        StringWriter writer = new StringWriter();
        StreamResult stream = new StreamResult(writer);

        Marshaller marshaller = marshallers.get(theMessage.getClass());
        if (marshaller == null) {
            throw new RuntimeException(
               "Configuration error: there isn't any marshalled configured for class:" + theMessage.getClass());
        }

        try {
            marshaller.marshal(theMessage, stream);
            result = writer.toString();
            writer.close();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return result;
    }
}