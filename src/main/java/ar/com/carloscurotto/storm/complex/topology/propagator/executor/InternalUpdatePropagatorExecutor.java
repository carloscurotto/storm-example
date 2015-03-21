package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;

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

public class InternalUpdatePropagatorExecutor extends AbstractUpdatePropagatorExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalUpdatePropagatorExecutor.class);
    private static final long serialVersionUID = 1L;

    public InternalUpdatePropagatorExecutor(UpdatePropagatorProvider thePropagatorProvider) {
        super(thePropagatorProvider);
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : update.getRows()) {
            ResultRow externalResultRow = externalResult.getRow(updateRow.getId());
            if (externalResultRow.isSuccessful() || externalResultRow.isSkipped()) {
                ResultRow resultRow = process(update, updateRow);
                resultRows.add(resultRow);
            } else {
                LOGGER.debug("Skipping internal update propagator for row [" + updateRow + "] on thread ["
                        + Thread.currentThread().getName() + "].");
                resultRows.add(ResultRow.skip(updateRow.getId()));
            }
        }
        theCollector.emit(new Values(new Result(resultRows)));
    }

}
