package ar.com.carloscurotto.storm.complex.transport.memory;

import java.io.Serializable;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareProducer;
import ar.com.carloscurotto.storm.complex.transport.memory.queue.InMemoryResultsQueue;

public class InMemoryResultProducer extends OpenAwareProducer<Result> implements Serializable {

    private static final long serialVersionUID = 1L;

    private InMemoryResultsQueue results = new InMemoryResultsQueue();

    @Override
    public void doSend(Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        results.put(theResult);
    }

    @Override
    protected void doOpen() {
        results.open();
    }

    @Override
    protected void doClose() {
        results.close();
    }
}
