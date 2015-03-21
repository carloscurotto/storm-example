package ar.com.carloscurotto.storm.complex.transport.print;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.Producer;

public class PrintProducer extends OpenAwareBean<ResultRow, Void> implements Producer, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintProducer.class);

    @Override
    public void send(ResultRow theResultRow) {
        doExecute(theResultRow);
    }

    @Override
    protected void doOpen() {
        LOGGER.info("Opening null producer");
    }

    @Override
    protected void doClose() {
        LOGGER.info("Closing null producer");
    }

    @Override
    protected Void doExecute(ResultRow theResultRow) {
        LOGGER.info("Sending message [" + theResultRow + "]");
        return null;
    }
}
