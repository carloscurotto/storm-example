package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import backtype.storm.tuple.Values;

public class ExternalUpdatePropagatorExecutor extends AbstractUpdatePropagatorExecutor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalUpdatePropagatorExecutor.class);
    private Set<String> skipableSystemIds;

    public ExternalUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider,
            final Set<String> theSkipableSystemIds) {
        super(thePropagatorProvider);
        Validate.notNull(theSkipableSystemIds, "The set of system ids to be skipped can not be null.");
        skipableSystemIds = theSkipableSystemIds;
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
        theCollector.emit(new Values(new Result(update.getId(), resultRows)));
    }

    private Collection<ResultRow> executePropagator(final Update theUpdate) {
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(theUpdate.getRows().size());
        for (UpdateRow updateRow : theUpdate.getRows()) {
            ResultRow resultRow = process(theUpdate, updateRow);
            resultRows.add(resultRow);
        }
        return resultRows;
    }

    private Collection<ResultRow> createSkipResultRows(final Update theUpdate) {
        LOGGER.debug("Skipping external update propagator for row [" + theUpdate + "] on thread ["
                + Thread.currentThread().getName() + "].");
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(theUpdate.getRows().size());
        for (UpdateRow updateRow : theUpdate.getRows()) {
            resultRows.add(ResultRow.skip(updateRow.getId()));
        }
        return resultRows;
    }

    private boolean shouldProcess(Update update) {
        return !skipableSystemIds.contains(update.getSystemId());
    }
}
