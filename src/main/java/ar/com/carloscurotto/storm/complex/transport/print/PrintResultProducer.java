package ar.com.carloscurotto.storm.complex.transport.print;

import java.io.Serializable;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.ResultProducer;

public class PrintResultProducer extends OpenAwareBean<Result, Void> implements ResultProducer, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintResultProducer.class);

    @Override
    public void send(Result theResult) {
        doExecute(theResult);
    }

    @Override
    protected void doOpen() {
        LOGGER.info("Opening print result producer");
    }

    @Override
    protected void doClose() {
        LOGGER.info("Closing print result producer");
    }

    @Override
    protected Void doExecute(Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        LOGGER.info("Sending result [" + theResult + "]");
        return null;
    }
}
