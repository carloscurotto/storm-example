package ar.com.carloscurotto.storm.complex.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.UpdateService;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ExternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.InternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ResultUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import ar.com.carloscurotto.storm.complex.transport.UpdateSubmitter;
import ar.com.carloscurotto.storm.complex.transport.memory.InMemoryUpdateSubmitter;
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
public class UpdatesTopologySubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatesTopologySubmitter.class);

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
            
            UpdateSubmitter submitter = new InMemoryUpdateSubmitter();
            UpdateService service = new UpdateService(submitter );
            service.open();
            Update firstUpdate = createUpdateFor("id-1", "SEMS", "row-1");
            Result firstResult = service.execute(firstUpdate);
            LOGGER.info("First result: " + firstResult);
            Update secondUpdate = createUpdateFor("id-2", "ANOTHER", "row-2");
            Result secondResult = service.execute(secondUpdate);
            LOGGER.info("Second result: " + secondResult);
            service.close();

            LOGGER.debug("Press any key to stop processing...");
            System.in.read();

            cluster.killTopology("complex-updates");

            cluster.shutdown();
        }

    }

    // TODO move this method somewhere else, it is repeated with FixedUpdatesSpout
    private static Update createUpdateFor(final String theId, final String theSystemId, final String theRowId) {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("parameter-key1", "parameter-value1");

        Collection<UpdateRow> rows = new ArrayList<UpdateRow>();
        Map<String, Object> keyColumns = new HashMap<String, Object>();

        keyColumns.put("key-column1", "key-value1");
        Map<String, Object> updateColumns = new HashMap<String, Object>();

        updateColumns.put("update-column1", "update-value1");

        UpdateRow row = new UpdateRow(theRowId, keyColumns, updateColumns);
        rows.add(row);

        return new Update(theId, theSystemId, "table", parameters, rows);
    }
    
}