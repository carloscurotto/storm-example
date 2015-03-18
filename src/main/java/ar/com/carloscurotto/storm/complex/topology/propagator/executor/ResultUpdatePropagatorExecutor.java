package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.Result;
import ar.com.carloscurotto.storm.complex.ResultRow;
import ar.com.carloscurotto.storm.complex.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.Update;
import ar.com.carloscurotto.storm.complex.UpdateRow;

public class ResultUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        Result externalResult = (Result) theTuple.getValueByField("external-result");
        Result internalResult = (Result) theTuple.getValueByField("internal-result");
        Collection<UpdateRow> updateRows = update.getRows();
        Collection<ResultRow> finalResultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : updateRows) {
            ResultRow externalResultRow = externalResult.getRow(updateRow.getId());
            ResultRow internalResultRow = internalResult.getRow(updateRow.getId());
            //TODO see if we can refactor this conditional logic
            if (externalResultRow.isSuccessful() && internalResultRow.isSuccessful()) {
                finalResultRows.add(new ResultRow(updateRow.getId(), ResultRowStatus.SUCCESS, "External and internal update sucessful."));
            } else if (externalResultRow.isSkipped() && internalResultRow.isSuccessful()) {
                finalResultRows.add(new ResultRow(updateRow.getId(), ResultRowStatus.SUCCESS, "External update skipped and internal update sucessful."));
            } else if (externalResultRow.isSuccessful() && internalResultRow.isFailure()) {
                finalResultRows.add(new ResultRow(updateRow.getId(), ResultRowStatus.SUCCESS, "External update successful and internal update failed."));
            } else if (externalResultRow.isFailure()) {
                finalResultRows.add(new ResultRow(updateRow.getId(), ResultRowStatus.FAILURE, "External update failed."));
            } else {
                throw new RuntimeException("Update results not supported [external="
                        + externalResultRow.getStatus().name() + ", internal= " + internalResultRow.getStatus().name()
                        + "]");
            }
        }
        //TODO send the final result through the transport layer
    }
    
}
