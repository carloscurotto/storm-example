package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;

/**
 * This is a producer that will write any gloss message to the underlying logging system. Intended for testing and demo processes.
 *
 * @author d540601
 */
public class GlossLoggerProducer extends OpenAwareProducer<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlossLoggerProducer.class);

    /**
     * Writes theContext to the console.
     *
     * @param theMessage
     *            a message to write to the logger.
     */
    @Override
    protected void doSend(String theMessage) {
        LOGGER.info(theMessage);
    }

    @Override
    protected void doOpen() {
        LOGGER.info("Opening producer...");
    }

    @Override
    protected void doClose() {
        LOGGER.info("Closing producer...");
    }

}
