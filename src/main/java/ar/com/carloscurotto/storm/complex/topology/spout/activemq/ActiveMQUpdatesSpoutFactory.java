package ar.com.carloscurotto.storm.complex.topology.spout.activemq;

import org.apache.commons.lang3.Validate;

import backtype.storm.topology.IRichSpout;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;

public class ActiveMQUpdatesSpoutFactory implements UpdatesSpoutFactory {
    
    private String brokerUrl;
    
    public ActiveMQUpdatesSpoutFactory(final String theBrokerUrl) {
        Validate.notBlank(theBrokerUrl, "The broker url can not be blank.");
        brokerUrl = theBrokerUrl;
    }

    @Override
    public IRichSpout create() {
        return new ActiveMQUpdatesSpout(brokerUrl);
    }
    
    
}
