package ar.com.carloscurotto.storm.complex.transport.memory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Update;

public class InMemoryUpdatesQueue {
    
    private BlockingQueue<Update> updates = new LinkedBlockingQueue<>();
    
    private static InMemoryUpdatesQueue INSTANCE = new InMemoryUpdatesQueue();
    
    private InMemoryUpdatesQueue() {
    }
    
    public static InMemoryUpdatesQueue getInstance() {
        return INSTANCE;
    }
    
    public void put(final Update theUpdate) {
        Validate.notNull(theUpdate, "The update can not be null.");
        try {
            updates.put(theUpdate);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error puting update.", e);
        }        
    }
    
    public Update take() {
        try {
            return updates.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error taking update.", e);
        }                
    }
    
    public Update poll() {
        return updates.poll();
    }
    
}
