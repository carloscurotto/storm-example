package ar.com.carloscurotto.storm.complex.transport.memory.queue;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.Openable;

import com.google.common.base.Preconditions;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class InMemoryUpdatesQueue implements Closeable, Openable, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private static final String UPDATES_QUEUE_NAME = "updates";
    private static final String HZ_CONFIGURATION_FILE_NAME = "hazelcast.xml";
    private static final String HZ_INSTANCE_NAME = "updates-hz-instance";    
    
    private boolean isOpen;
    
    @Override
    public void open() {
        Preconditions.checkState(!isOpen(), "The service is already opened. Cannot open more than once.");
        getHazelcastInstance();
        isOpen = true;        
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() {
        if (isOpen()) {
            getHazelcastInstance().shutdown();
            isOpen = false;
        }        
    }    
    
    private HazelcastInstance getHazelcastInstance() {
        Config configuration = new ClasspathXmlConfig(HZ_CONFIGURATION_FILE_NAME);
        configuration.setInstanceName(HZ_INSTANCE_NAME);
        return Hazelcast.getOrCreateHazelcastInstance(configuration);        
    }
    
    private BlockingQueue<Update> getUpdates() {
        return getHazelcastInstance().getQueue(UPDATES_QUEUE_NAME);
    }    
    
    public void put(final Update theUpdate) {
        Validate.notNull(theUpdate, "The update can not be null.");
        try {
            getUpdates().put(theUpdate);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error puting update.", e);
        }        
    }
    
    public Update take() {
        try {
            return getUpdates().take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error taking update.", e);
        }                
    }
    
    public Update poll() {
        return getUpdates().poll();
    }
    
}