package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.TradeMessage;

public class GlossMessageMarshaller {

    private Map<Class<? extends TradeMessage>, Marshaller> marshallers = new HashMap<Class<? extends TradeMessage>, Marshaller>();
    private List<Class<? extends TradeMessage>> messageClasses;

    /**
     * Constructs the GlossMessageMarshaller with the given list of message classes.
     *
     * @param theMessageClasses
     *            a {@link List<Class<? extends TradeMessage>>} with the classes (.class) whose data this message sender
     *            will send. It cannot be null nor empty.
     */
    public GlossMessageMarshaller(final List<Class<? extends TradeMessage>> theMessageClasses) {
        Validate.notEmpty(theMessageClasses, "The message classes list cannot be empty");
        messageClasses = theMessageClasses;
        initializeMarshallers();
    }

    private void initializeMarshallers() {
        try {
            for (Class<? extends TradeMessage> clazz : messageClasses) {
                marshallers.put(clazz, JAXBContext.newInstance(clazz).createMarshaller());
            }
        } catch (JAXBException e) {
            throw new RuntimeException(
                    "MessageSender can't create the marshallers needed to convert the trade objects to xml", e);
        }
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
    public String marshal(final TradeMessage theMessage) {
        Validate.notNull(theMessage, "The message cannot be null");

        String result = null;
        StringWriter writer = new StringWriter();
        StreamResult stream = new StreamResult(writer);

        Marshaller marshaller = marshallers.get(theMessage.getClass());
        if (marshaller == null) {
            throw new RuntimeException("Configuration error: there isn't any marshalled configured for class:"
                    + theMessage.getClass());
        }

        try {
            marshaller.marshal(theMessage, stream);
            result = writer.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(writer);
        }

        return result;
    }
}
