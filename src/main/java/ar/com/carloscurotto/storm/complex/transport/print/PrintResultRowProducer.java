package ar.com.carloscurotto.storm.complex.transport.print;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.transport.Producer;

public class PrintResultRowProducer implements Producer<ResultRow>, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintResultRowProducer.class);
    
    @Override
    public void open() {
        LOGGER.info("Opening null producer");
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() {
        LOGGER.info("Closing null producer");
    }

    @Override
    public void send(ResultRow theResultRow) {
        LOGGER.info("Sending message [" + theResultRow + "]");
    }
    
}
