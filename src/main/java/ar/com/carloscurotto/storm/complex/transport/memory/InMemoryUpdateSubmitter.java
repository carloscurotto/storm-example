package ar.com.carloscurotto.storm.complex.transport.memory;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.Submitter;
import ar.com.carloscurotto.storm.complex.transport.memory.queue.InMemoryResultsQueue;
import ar.com.carloscurotto.storm.complex.transport.memory.queue.InMemoryUpdatesQueue;

public class InMemoryUpdateSubmitter extends OpenAwareBean<Update, Result> implements Submitter<Update, Result> {

    private InMemoryResultsQueue results = new InMemoryResultsQueue();
    private InMemoryUpdatesQueue updates = new InMemoryUpdatesQueue();

    @Override
    public Result submit(final Update theUpdate) {
        return doExecute(theUpdate);
    }

    @Override
    protected void doOpen() {
        results.open();
        updates.open();
    }

    @Override
    protected void doClose() {
        updates.close();
        results.close();
    }

    @Override
    protected Result doExecute(final Update theUpdate) {
        Validate.notNull(theUpdate, "The update can not be null.");
        put(theUpdate);
        return take(theUpdate);
    }

    private void put(final Update theUpdate) {
        updates.put(theUpdate);
    }

    private Result take(final Update theUpdate) {
        Result theResult = results.take();
        while (!theResult.getId().equals(theUpdate.getId())) {
            results.put(theResult);
            theResult = results.take();
        }
        return theResult;
    }

}
