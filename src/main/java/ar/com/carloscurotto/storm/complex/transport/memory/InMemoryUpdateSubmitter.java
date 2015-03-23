package ar.com.carloscurotto.storm.complex.transport.memory;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.transport.UpdateSubmitter;

public class InMemoryUpdateSubmitter extends OpenAwareBean<Update, Result> implements UpdateSubmitter {
    
    @Override
    public Result submit(final Update theUpdate) {
        return doExecute(theUpdate);
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected Result doExecute(final Update theUpdate) {
        Validate.notNull(theUpdate, "The update can not be null.");
        put(theUpdate);
        return take(theUpdate);
    }
    
    private void put(final Update theUpdate) {
        InMemoryUpdatesQueue.getInstance().put(theUpdate);
    }
    
    private Result take(final Update theUpdate) {
        Result theResult = InMemoryResultsQueue.getInstance().take();
        while (!theResult.getId().equals(theUpdate.getId())) {
            InMemoryResultsQueue.getInstance().put(theResult);
            theResult = InMemoryResultsQueue.getInstance().take();
        }
        return theResult;
    }

}
