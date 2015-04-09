package ar.com.carloscurotto.storm.updates.trident.repository;

import java.io.Serializable;
import java.util.Set;

import com.hazelcast.core.IMap;

public abstract class AbstractUpdateCountsRepository implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String EQUAL_OPERATOR = "=";

    protected abstract String getMapName();

    public void start() {
        HazelcastInstanceProvider.start();
    }

    public void stop() {
        HazelcastInstanceProvider.stop();
    }

    public void put(String update, Integer count) {
        HazelcastInstanceProvider.getHazelcastInstance().getMap(getMapName()).put(update, count);
    }

    public Integer get(String update) {
        return (Integer) HazelcastInstanceProvider.getHazelcastInstance().getMap(getMapName()).get(update);
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        IMap<String, Integer> counts = HazelcastInstanceProvider.getHazelcastInstance().getMap(getMapName());
        Set<String> keys = counts.keySet();
        buffer.append("[" + getMapName() + "]").append(System.lineSeparator());
        for (String key : keys) {
            buffer.append(key).append(EQUAL_OPERATOR).append(counts.get(key)).append(System.lineSeparator());
        }
        return buffer.toString();
    }
}
