package ar.com.carloscurotto.storm.complex.transport.print;

import java.io.Serializable;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;

public class PrintResultProducer extends OpenAwareProducer<Result> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintResultProducer.class);

    @Override
    public void doSend(Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        LOGGER.info("Sending result [" + theResult + "]");
    }

    @Override
    protected void doOpen() {
        LOGGER.info("Opening print result producer");
    }

    @Override
    protected void doClose() {
        LOGGER.info("Closing print result producer");
    }
}
