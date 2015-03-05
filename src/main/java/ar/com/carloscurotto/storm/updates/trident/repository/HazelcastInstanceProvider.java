package ar.com.carloscurotto.storm.updates.trident.repository;

import java.io.Serializable;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastInstanceProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String HZ_CONFIGURATION_FILE_NAME = "hazelcast.xml";
    private static final String HZ_INSTANCE_NAME = "update-count-hz-instance";

    public static void start() {
        Config configuration = new ClasspathXmlConfig(HZ_CONFIGURATION_FILE_NAME);
        configuration.setInstanceName(HZ_INSTANCE_NAME);
        Hazelcast.getOrCreateHazelcastInstance(configuration);
    }

    public static void stop() {
        HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName(HZ_INSTANCE_NAME);
        if (hz != null) {
            hz.shutdown();
        }
    }

    public static HazelcastInstance getHazelcastInstance() {
        return Hazelcast.getHazelcastInstanceByName(HZ_INSTANCE_NAME);
    }

}
