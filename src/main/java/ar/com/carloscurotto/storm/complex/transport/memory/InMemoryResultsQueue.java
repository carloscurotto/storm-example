package ar.com.carloscurotto.storm.complex.transport.memory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;

public class InMemoryResultsQueue {
    
    private BlockingQueue<Result> results = new LinkedBlockingQueue<>();
    
    private static InMemoryResultsQueue INSTANCE = new InMemoryResultsQueue();
    
    private InMemoryResultsQueue() {
    }
    
    public static InMemoryResultsQueue getInstance() {
        return INSTANCE;
    }
    
    public void put(final Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        try {
            results.put(theResult);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error puting result.", e);
        }        
    }
    
    public Result take() {
        try {
            return results.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error taking result.", e);
        }                
    }
    
}
