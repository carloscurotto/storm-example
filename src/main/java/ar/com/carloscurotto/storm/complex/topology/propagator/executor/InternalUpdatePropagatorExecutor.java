package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;

public class InternalUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private UpdatePropagatorProvider propagatorProvider;

    public InternalUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider) {
        Validate.notNull(thePropagatorProvider, "The propagator provider can not be null.");
        propagatorProvider = thePropagatorProvider;
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
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Collection<UpdateRow> updateRows = update.getRows();
        Collection<ResultRow> internalResultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : updateRows) {
            ResultRow externalResultRow = externalResult.getRow(updateRow.getId());
            if (externalResultRow.isSuccessful() || externalResultRow.isSkipped()) {
                OpenAwareService<UpdatePropagatorContext, ResultRow> propagator =
                        propagatorProvider.getPropagator(update.getSystemId());
                internalResultRows.add(propagator.execute(new UpdatePropagatorContext(update.getTableName(), updateRow, update
                        .getParameters())));
            } else {
                internalResultRows
                        .add(new ResultRow(updateRow.getId(), ResultRowStatus.SKIPPED, "Update row skipped."));
            }
        }
        theCollector.emit(new Values(new Result(internalResultRows)));
    }

}
