package ar.com.carloscurotto.storm.wordcount.serialized;

import java.io.Serializable;
import java.util.Set;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class WordCountsRepository implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String EQUAL_OPERATOR = "=";
    private static final String HZ_CONFIGURATION_FILE_NAME = "hazelcast.xml";
    private static final String HZ_INSTANCE_NAME = "word-count-hz-instance";
    private static final String HZ_MAP_NAME = "word-count-map";

    public synchronized void start() {
        Config configuration = new ClasspathXmlConfig(HZ_CONFIGURATION_FILE_NAME);
        configuration.setInstanceName(HZ_INSTANCE_NAME);
        Hazelcast.getOrCreateHazelcastInstance(configuration);
    }

    public synchronized void stop() {
        HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName(HZ_INSTANCE_NAME);
        if (hz != null) {
            hz.shutdown();
        }
    }

    private HazelcastInstance getHazelcastInstance() {
        return Hazelcast.getHazelcastInstanceByName(HZ_INSTANCE_NAME);
    }

    public void put(String word, Integer count) {
        getHazelcastInstance().getMap(HZ_MAP_NAME).put(word, count);
    }

    public Integer get(String word) {
        return (Integer) getHazelcastInstance().getMap(HZ_MAP_NAME).get(word);
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        IMap<String, Integer> counts = getHazelcastInstance().getMap(HZ_MAP_NAME);
        Set<String> keys = counts.keySet();
        for (String key : keys) {
            buffer.append(key).append(EQUAL_OPERATOR).append(counts.get(key)).append(System.lineSeparator());
        }
        return buffer.toString();
    }
}
