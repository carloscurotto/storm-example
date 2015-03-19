package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;

public class ResultUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Result internalResult = (Result) theTuple.getValueByField("internal-result");

        Collection<ResultRow> finalResultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : update.getRows()) {
            ResultRow externalResultRow = externalResult.getRow(updateRow.getId());
            ResultRow internalResultRow = internalResult.getRow(updateRow.getId());
            finalResultRows.add(createFinalResultRow(externalResultRow, internalResultRow));
        }
        // TODO send the final result through the transport layer
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
