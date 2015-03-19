package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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

public class InternalUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalUpdatePropagatorExecutor.class);

    private UpdatePropagatorProvider propagatorProvider;

    public InternalUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider) {
        Validate.notNull(thePropagatorProvider, "The propagator provider can not be null.");
        propagatorProvider = thePropagatorProvider;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TridentOperationContext theContext) {
        LOGGER.debug("Opening internal update propagator executor");
        propagatorProvider.open();
    }

    @Override
    public void cleanup() {
        LOGGER.debug("Closing internal update propagator executor");
        propagatorProvider.close();
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : update.getRows()) {
            ResultRow externalResultRow = externalResult.getRow(updateRow.getId());
            if (externalResultRow.isSuccessful() || externalResultRow.isSkipped()) {
                AbstractUpdatePropagator propagator = propagatorProvider.getPropagator(update.getSystemId());
                LOGGER.debug("Executing internal update propagator for row [" + updateRow + "] on thread ["
                        + Thread.currentThread().getName() + "].");
                UpdatePropagatorResult updatePropagatorResult =
                        propagator.execute(new UpdatePropagatorContext(update.getTableName(), updateRow, update
                                .getParameters()));
                resultRows.add(ResultRow.from(updateRow.getId(), updatePropagatorResult));
            } else {
                LOGGER.debug("Skipping internal update propagator for row [" + updateRow + "] on thread ["
                        + Thread.currentThread().getName() + "].");                            
                resultRows.add(ResultRow.skip(updateRow.getId()));
            }
        }
        theCollector.emit(new Values(new Result(resultRows)));
    }

}
