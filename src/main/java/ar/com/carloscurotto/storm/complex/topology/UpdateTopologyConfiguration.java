package ar.com.carloscurotto.storm.complex.topology;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import ar.com.carloscurotto.storm.complex.service.UpdateService;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ExternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.InternalUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.executor.ResultUpdatePropagatorExecutor;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Container for all the beans that are necessary for creating the storm topology.
 *
 * @author O605461
 *
 */
public class UpdateTopologyConfiguration implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTopologyConfiguration.class);

    @Autowired
    private UpdatesSpoutFactory spoutFactory;
    @Autowired
    private ExternalUpdatePropagatorExecutor externalPropagatorExecutor;
    @Autowired
    private InternalUpdatePropagatorExecutor internalPropagatorExecutor;
    @Autowired
    private ResultUpdatePropagatorExecutor resultUpdatePropagatorExecutor;
    @Autowired
    private UpdateService updateService;

    private StormTopology stormTopology;

    /**
     * Constructor for spring only purposes.
     */
    @Deprecated
    public UpdateTopologyConfiguration() {
    }

    public UpdateTopologyConfiguration(UpdatesSpoutFactory theSpoutFactory,
            ExternalUpdatePropagatorExecutor theExternalPropagatorExecutor,
            InternalUpdatePropagatorExecutor theInternalPropagatorExecutor,
            ResultUpdatePropagatorExecutor theResultUpdatePropagatorExecutor, UpdateService theUpdateService) {

        Validate.notNull(theSpoutFactory, "The spout factory cannot be null.");
        Validate.notNull(theExternalPropagatorExecutor, "The external propagator executor cannot be null.");
        Validate.notNull(theInternalPropagatorExecutor, "The internal propagator executor cannot be null.");
        Validate.notNull(theResultUpdatePropagatorExecutor, "The result update propagator executor cannot be null.");
        Validate.notNull(theUpdateService, "The update service cannot be null.");
        spoutFactory = theSpoutFactory;
        externalPropagatorExecutor = theExternalPropagatorExecutor;
        internalPropagatorExecutor = theInternalPropagatorExecutor;
        resultUpdatePropagatorExecutor = theResultUpdatePropagatorExecutor;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        LOGGER.debug("Setting up trident topology...");

        TridentTopology trident = new TridentTopology();
        Stream updatesStream = trident.newStream("updates-spout", spoutFactory.create()).parallelismHint(1).shuffle();
        updatesStream = updatesStream
                .each(new Fields("update"), externalPropagatorExecutor, new Fields("external-result"))
                .parallelismHint(1).partitionBy(new Fields("update"));
        updatesStream = updatesStream
                .each(new Fields("update", "external-result"), internalPropagatorExecutor,
                        new Fields("internal-result")).parallelismHint(1).partitionBy(new Fields("update"));
        updatesStream.each(new Fields("update", "external-result", "internal-result"), resultUpdatePropagatorExecutor,
                new Fields("final-result")).parallelismHint(1);
        updatesStream.name("external-internal-updates");

        LOGGER.debug("Building topology...");
        stormTopology = trident.build();
        LOGGER.debug("Finished building topology...");
    }

    public StormTopology getStormTopology() {
        return stormTopology;
    }
}
