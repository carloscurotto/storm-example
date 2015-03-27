package ar.com.carloscurotto.storm.complex.topology.spout.activemq;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import ar.com.carloscurotto.storm.complex.transport.activemq.ActiveMQConfiguration;
import backtype.storm.topology.IRichSpout;

public class ActiveMQUpdatesSpoutFactory implements UpdatesSpoutFactory {

    private ActiveMQConfiguration activeMQConfiguration;

    public ActiveMQUpdatesSpoutFactory(final ActiveMQConfiguration theActiveMQConfiguration) {
        Validate.notNull(theActiveMQConfiguration, "The activeMQ configuration cannot be null");
        activeMQConfiguration = theActiveMQConfiguration;
    }

    @Override
    public IRichSpout create() {
        return new ActiveMQUpdatesSpout(activeMQConfiguration);
    }
}
