package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import java.io.Serializable;
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

import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

public class GlossMessageMarshaller extends OpenAwareBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<Class<? extends GlossMessage>, Marshaller> marshallers = new HashMap<Class<? extends GlossMessage>, Marshaller>();
    private List<Class<? extends GlossMessage>> messageClasses;

    public GlossMessageMarshaller(final List<Class<? extends GlossMessage>> theMessageClasses) {
        Validate.notEmpty(theMessageClasses, "The message classes list cannot be empty");
        messageClasses = theMessageClasses;
    }

    @Override
    protected void doOpen() {
        try {
            for (Class<? extends GlossMessage> clazz : messageClasses) {
                marshallers.put(clazz, JAXBContext.newInstance(clazz).createMarshaller());
            }
        } catch (JAXBException e) {
            throw new RuntimeException(
                    "MessageSender can't create the marshallers needed to convert the trade objects to xml", e);
        }
    }

    @Override
    protected void doClose() {
        marshallers = null;
    }

    public String marshal(final GlossMessage theMessage) {
        validateIsOpened();
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
