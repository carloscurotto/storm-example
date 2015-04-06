package ar.com.carloscurotto.storm.complex.topology;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

/**
 * This is the main class that knows how to submit our topology.
 *
 * @author O605461
 */
public class UpdateTopologyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTopologyRunner.class);

    @Autowired
    private UpdateTopologyConfiguration updateTopologyConfiguration;

    /**
     * Constructor for spring only purposes.
     */
    @Deprecated
    public UpdateTopologyRunner() {
    }

    public UpdateTopologyRunner(final UpdateTopologyConfiguration theUpdateTopologyConfiguration) {
        Validate.notNull(theUpdateTopologyConfiguration, "The updates topology configuration cannot be null.");
        updateTopologyConfiguration = theUpdateTopologyConfiguration;
    }

    /**
     * Submits our topology to the cluster.
     *
     */
    public static void main(String[] args) throws Exception {
        LOGGER.debug("Running the topology...");

        @SuppressWarnings("resource")
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("app-config.xml");
        UpdateTopologyRunner topologyRunner = applicationContext.getBean(UpdateTopologyRunner.class);
        StormTopology topology = topologyRunner.updateTopologyConfiguration.getStormTopology();

        Config config = new Config();
        config.setDebug(false);

        LOGGER.debug("Deploying topology remotely...");
        StormSubmitter.submitTopologyWithProgressBar("complex-updates", config, topology);
    }
}