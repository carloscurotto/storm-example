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
public class UpdatesTopologyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatesTopologyRunner.class);

    @Autowired
    private UpdateTopologyConfiguration updateTopologyConfiguration;

    /**
     * Constructor for spring only purposes.
     */
    @Deprecated
    public UpdatesTopologyRunner() {
    }

    public UpdatesTopologyRunner(final UpdateTopologyConfiguration theUpdateTopologyConfiguration) {
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
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("complex-context.xml");
        UpdatesTopologyRunner topologyRunner = applicationContext.getBean(UpdatesTopologyRunner.class);
        StormTopology topology = topologyRunner.updateTopologyConfiguration.getStormTopology();

        Config config = new Config();
        config.setDebug(false);

        LOGGER.debug("Deploying topology remotely...");

        StormSubmitter.submitTopologyWithProgressBar("complex-updates", config, topology);
        // LOGGER.debug("Deploying topology locally...");
        //
        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology("complex-updates", config, topology);
        //
        // LOGGER.debug("Press any key to stop processing...");
        // System.in.read();
        //
        // cluster.killTopology("complex-updates");
        //
        // cluster.shutdown();
    }
}