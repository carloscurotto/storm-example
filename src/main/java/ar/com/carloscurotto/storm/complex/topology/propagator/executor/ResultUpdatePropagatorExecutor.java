package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.Validate;
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
import ar.com.carloscurotto.storm.complex.transport.ResultProducer;

public class ResultUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultUpdatePropagatorExecutor.class);

    private ResultProducer producer;

    public ResultUpdatePropagatorExecutor(final ResultProducer theResultProducer) {
        Validate.notNull(theResultProducer, "The result producer can not be null.");
        producer = theResultProducer;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TridentOperationContext theContext) {
        LOGGER.debug("Opening result update propagator executor");
        producer.open();
    }

    @Override
    public void cleanup() {
        LOGGER.debug("Closing result update propagator executor");
        producer.close();
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Result internalResult = (Result) theTuple.getValueByField("internal-result");

        sendFinalResult(createFinalResult(update, externalResult, internalResult));
    }

    private void sendFinalResult(final Result theFinalResult) {
        producer.send(theFinalResult);
    }

    private Result createFinalResult(final Update theUpdate, final Result theExternalResult,
            final Result theInternalResult) {
        Collection<ResultRow> finalResultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : theUpdate.getRows()) {
            LOGGER.debug("Executing result update propagator executor for row [" + updateRow + "] on thread ["
                    + Thread.currentThread().getName() + "].");
            ResultRow externalResultRow = theExternalResult.getRow(updateRow.getId());
            ResultRow internalResultRow = theInternalResult.getRow(updateRow.getId());
            finalResultRows.add(createFinalResultRow(externalResultRow, internalResultRow));
        }
        return new Result(theUpdate.getId(), finalResultRows);
    }

    private ResultRow createFinalResultRow(final ResultRow externalResultRow, final ResultRow internalResultRow) {
        if (externalResultRow.isSuccessful() && internalResultRow.isSuccessful()) {
            return ResultRow.success(externalResultRow.getId(), "External and internal update sucessful.");
        } else if (externalResultRow.isSkipped() && internalResultRow.isSuccessful()) {
            return ResultRow.success(externalResultRow.getId(),
                    "External update skipped and internal update sucessful.");
        } else if (externalResultRow.isSuccessful() && internalResultRow.isFailure()) {
            return ResultRow.success(externalResultRow.getId(),
                    "External update successful and internal update failed.");
        } else if (externalResultRow.isFailure()) {
            return ResultRow.failure(externalResultRow.getId(), "External update failed.");
        } else {
            throw new RuntimeException("Update results not supported [external=" + externalResultRow + ", internal= "
                    + internalResultRow + "]");
        }
    }

}
