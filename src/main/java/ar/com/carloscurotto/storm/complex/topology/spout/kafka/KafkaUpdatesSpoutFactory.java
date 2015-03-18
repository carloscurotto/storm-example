package ar.com.carloscurotto.storm.complex.topology.spout.kafka;

import org.apache.commons.lang3.StringUtils;

import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;

import com.google.common.base.Preconditions;

/**
 * This class knows how to create spouts connected to a kafka broker on a specific topic.
 *
 * @author O605461
 *
 */
public class KafkaUpdatesSpoutFactory implements UpdatesSpoutFactory {

    /**
     * The hosts to connect to.
     */
    private String hosts;

    /**
     * The topic to connect to.
     */
    private String topic;

    /**
     * Creates a {@link KafkaUpdatesSpoutFactory}.
     *
     * @param theHosts
     *            the hosts to connect this spout to.
     * @param theTopic
     *            the topic to connect this spout to.
     */
    public KafkaUpdatesSpoutFactory(final String theHosts, final String theTopic) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(theHosts), "The hosts can not be blank.");
        Preconditions
                .checkArgument(StringUtils.isNotBlank(theTopic), "The topic can not be blank.");
        hosts = theHosts;
        topic = theTopic;
    }

    /**
     * Creates a {@link TransactionalTridentKafkaSpout} properly configured.
     *
     * @return the created spout.
     */
    public IRichSpout create() {
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(new ZkHosts(hosts), topic);
        spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaUpdatesScheme());
        return (IRichSpout) new TransactionalTridentKafkaSpout(spoutConfig);
    }

}