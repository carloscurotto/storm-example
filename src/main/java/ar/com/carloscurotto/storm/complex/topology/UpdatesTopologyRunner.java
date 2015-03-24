package ar.com.carloscurotto.storm.complex.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ExternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.InternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ResultUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * This is the main class that knows how to submit our topology.
 *
 * @author O605461
 */
public class UpdatesTopologyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatesTopologyRunner.class);

    /**
     * Submits our topology to the cluster.
     *
     * @param args
     *            the given arguments.
     */
    public static void main(String[] args) throws Exception {

    	LOGGER.debug("Loading configuration...");

    	UpdatesTopologyConfiguration configuration = new UpdatesTopologyConfiguration();

    	LOGGER.debug("Getting beans...");

    	UpdatesSpoutFactory spoutFactory = configuration.getUpdatesSpoutFactory();
    	ExternalUpdatePropagatorExecutor externalPropagator = configuration.getExternalUpdatePropagatorExecutor();
    	InternalUpdatePropagatorExecutor internalPropagator = configuration.getInternalUpdatePropagatorExecutor();
    	ResultUpdatePropagatorExecutor resultPropagator = configuration.getResultUpdatePropagatorExecutor();

    	LOGGER.debug("Creating topology...");

        TridentTopology trident = new TridentTopology();

        Stream updatesStream = trident.newStream("updates-spout", spoutFactory.create()).parallelismHint(1).shuffle();
        updatesStream =
                updatesStream.each(new Fields("update"), externalPropagator, new Fields("external-result"))
                        .parallelismHint(1).partitionBy(new Fields("update"));
        updatesStream =
                updatesStream
                        .each(new Fields("update", "external-result"), internalPropagator,
                                new Fields("internal-result")).parallelismHint(1)
                        .partitionBy(new Fields("update"));
        updatesStream.each(new Fields("update", "external-result", "internal-result"), resultPropagator,
                new Fields("final-result")).parallelismHint(1);
        updatesStream.name("external-internal-updates");

        LOGGER.debug("Building topology...");

        StormTopology topology = trident.build();

        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            LOGGER.debug("Deploying topology remotely...");

            StormSubmitter.submitTopologyWithProgressBar("complex-updates", config, topology);
        } else {
            LOGGER.debug("Deploying topology locally...");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("complex-updates", config, topology);
            
            LOGGER.debug("Press any key to stop processing...");
            System.in.read();

            cluster.killTopology("complex-updates");

            cluster.shutdown();
        }

    }
    
}