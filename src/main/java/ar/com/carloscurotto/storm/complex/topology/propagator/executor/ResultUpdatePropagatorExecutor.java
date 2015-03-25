package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.Collection;
import java.util.LinkedList;
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
import ar.com.carloscurotto.storm.complex.model.ResultRowMessageResolver;
import ar.com.carloscurotto.storm.complex.model.Update;
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
        Result finalResult = collectResults(theTuple);
        producer.send(finalResult);
    }

    private Result collectResults(final TridentTuple theTuple) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Result internalResult = (Result) theTuple.getValueByField("internal-result");

        return createFinalResult(update, externalResult, internalResult);
    }

    private Result createFinalResult(final Update theUpdate, final Result theExternalResult,
            final Result theInternalResult) {
        Collection<ResultRow> finalResultRows = new LinkedList<ResultRow>();
        for (String updateRowId : theUpdate.getRowsId()) {
            LOGGER.debug("Executing result update propagator executor for row [" + updateRowId + "] on thread ["
                    + Thread.currentThread().getName() + "].");
            ResultRow externalResultRow = theExternalResult.getRow(updateRowId);
            ResultRow internalResultRow = theInternalResult.getRow(updateRowId);
            finalResultRows.add(createResult(externalResultRow, internalResultRow));
        }
        return new Result(theUpdate.getId(), finalResultRows);
    }

    private ResultRow createResult(final ResultRow theExternalResultRow, final ResultRow theInternalResultRow) {
        String message = ResultRowMessageResolver.getMessage(theExternalResultRow, theInternalResultRow);
        if (isFailure(theExternalResultRow, theInternalResultRow)) {
            return ResultRow.failure(theExternalResultRow.getId(), message);
        }

        return ResultRow.success(theExternalResultRow.getId(), message);
    }

    private boolean isFailure(final ResultRow theExternalResult, final ResultRow theInternalResult) {
        return theExternalResult.isFailure() || (theExternalResult.isSkipped() && theInternalResult.isFailure());
    }
}
