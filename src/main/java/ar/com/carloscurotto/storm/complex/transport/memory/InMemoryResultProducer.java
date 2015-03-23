package ar.com.carloscurotto.storm.complex.transport.memory;

import java.io.Serializable;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.ResultProducer;

public class InMemoryResultProducer extends OpenAwareBean<Result, Void> implements ResultProducer, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public void send(Result theResult) {
        doExecute(theResult);
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected Void doExecute(Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        InMemoryResultsQueue.getInstance().put(theResult);
        return null;
    }
}
