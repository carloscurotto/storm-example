package ar.com.carloscurotto.storm.complex.transport.activemq;

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQBrokerRunner {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQBrokerRunner.class);
    
    private static final String BROKER_URL = "tcp://localhost:61616";
    
    public static void main(String[] args) {
        try {
            BrokerService broker = new BrokerService();
            broker.setPersistent(false);
            broker.setUseJmx(false);
            broker.addConnector(BROKER_URL);
            broker.start();
            LOGGER.info("Active MQ Broker started at [" + BROKER_URL + "]. Press any key to stop it...");
            System.in.read();
        } catch (Exception e) {
            throw new RuntimeException("Error starting active mq broker.", e);
        }
    }
    
}
