package ar.com.carloscurotto.storm.complex.transport.memory.queue;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class InMemoryResultsQueue extends OpenAwareBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String RESULTS_QUEUE_NAME = "results";
    private static final String HZ_CONFIGURATION_FILE_NAME = "hazelcast.xml";
    private static final String HZ_INSTANCE_NAME = "updates-hz-instance";

    @Override
    public void doOpen() {
        getHazelcastInstance();
    }

    @Override
    public void doClose() {
        getHazelcastInstance().shutdown();
    }

    private HazelcastInstance getHazelcastInstance() {
        Config configuration = new ClasspathXmlConfig(HZ_CONFIGURATION_FILE_NAME);
        configuration.setInstanceName(HZ_INSTANCE_NAME);
        return Hazelcast.getOrCreateHazelcastInstance(configuration);
    }

    private BlockingQueue<Result> getResults() {
        return getHazelcastInstance().getQueue(RESULTS_QUEUE_NAME);
    }

    public void put(final Result theResult) {
        Validate.notNull(theResult, "The result can not be null.");
        try {
            getResults().put(theResult);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error puting result.", e);
        }
    }

    public Result take() {
        try {
            return getResults().take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error taking result.", e);
        }
    }

}
