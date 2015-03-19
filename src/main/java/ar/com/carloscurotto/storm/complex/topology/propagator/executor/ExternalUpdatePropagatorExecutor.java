package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;
import backtype.storm.tuple.Values;

public class ExternalUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalUpdatePropagatorExecutor.class);

    private UpdatePropagatorProvider propagatorProvider;

    private Set<String> skipableSystemIds;

    public ExternalUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider,
            final Set<String> theSkipableSystemIds) {
        Validate.notNull(thePropagatorProvider, "The propagator provider can not be null.");
        Validate.notNull(theSkipableSystemIds,
                "The set of system ids to be skipped can not be null.");
        propagatorProvider = thePropagatorProvider;
        skipableSystemIds = theSkipableSystemIds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TridentOperationContext theContext) {
        LOGGER.debug("Opening external update propagator executor");
        propagatorProvider.open();
    }

    @Override
    public void cleanup() {
        LOGGER.debug("Closing external update propagator executor");
        propagatorProvider.close();
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        final boolean shouldProcess = shouldProcess(update);
        Collection<ResultRow> resultRows;
        if (shouldProcess) {
            resultRows = executePropagator(update);
        } else {
            resultRows = createSkipResultRows(update);
        }
        theCollector.emit(new Values(new Result(resultRows)));
    }

    private Collection<ResultRow> executePropagator(final Update theUpdate) {
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(theUpdate.getRows().size());
        for (UpdateRow updateRow : theUpdate.getRows()) {
            AbstractUpdatePropagator propagator = propagatorProvider.getPropagator(theUpdate.getSystemId());
            LOGGER.debug("Executing external update propagator for row [" + updateRow + "] on thread ["
                    + Thread.currentThread().getName() + "].");
            UpdatePropagatorResult updatePropagatorResult =
                    propagator.execute(new UpdatePropagatorContext(theUpdate.getTableName(), updateRow, theUpdate
                            .getParameters()));
            resultRows.add(ResultRow.from(updateRow.getId(), updatePropagatorResult));
        }
        return resultRows;
    }

    private Collection<ResultRow> createSkipResultRows(final Update theUpdate) {
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(theUpdate.getRows().size());
        for (UpdateRow updateRow : theUpdate.getRows()) {
            LOGGER.debug("Skipping external update propagator for row [" + theUpdate + "] on thread ["
                    + Thread.currentThread().getName() + "].");            
            resultRows.add(ResultRow.skip(updateRow.getId()));
        }
        return resultRows;
    }

    private boolean shouldProcess(Update update) {
        return !skipableSystemIds.contains(update.getSystemId());
    }

}
