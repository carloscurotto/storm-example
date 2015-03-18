package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;
import backtype.storm.tuple.Values;

public class ExternalUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;

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
        propagatorProvider.open();
    }

    @Override
    public void cleanup() {
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
        Collection<UpdateRow> updateRows = theUpdate.getRows();
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(updateRows.size());
        for (UpdateRow updateRow : updateRows) {
            OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> propagator = propagatorProvider
                    .getPropagator(theUpdate.getSystemId());
            UpdatePropagatorResult updatePropagatorResult = propagator
                    .execute(new UpdatePropagatorContext(theUpdate.getTableName(), updateRow,
                            theUpdate.getParameters()));
            resultRows.add(ResultRow.from(updateRow.getId(), updatePropagatorResult));
        }
        return resultRows;

    }

    private Collection<ResultRow> createSkipResultRows(final Update theUpdate) {
        Collection<UpdateRow> updateRows = theUpdate.getRows();
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>(updateRows.size());
        for (UpdateRow updateRow : updateRows) {
            resultRows.add(ResultRow.skip(updateRow.getId()));
        }
        return resultRows;
    }

    private boolean shouldProcess(Update update) {
        return !skipableSystemIds.contains(update.getSystemId());
    }

}
