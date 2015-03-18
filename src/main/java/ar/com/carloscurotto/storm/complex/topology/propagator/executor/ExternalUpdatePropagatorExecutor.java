package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import ar.com.carloscurotto.storm.complex.Result;
import ar.com.carloscurotto.storm.complex.ResultRow;
import ar.com.carloscurotto.storm.complex.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.Update;
import ar.com.carloscurotto.storm.complex.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.route.UpdateRoute;
import ar.com.carloscurotto.storm.complex.topology.route.provider.UpdateRouteProvider;
import backtype.storm.tuple.Values;

public class ExternalUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    
    private UpdatePropagatorProvider propagatorpProvider;
    
    private UpdateRouteProvider routeProvider;

    public ExternalUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider,
            final UpdateRouteProvider theRouteProvider) {
        Validate.notNull(thePropagatorProvider, "The propagator provider can not be null.");
        Validate.notNull(theRouteProvider, "The route provider can not be null.");
        propagatorpProvider = thePropagatorProvider;
        routeProvider = theRouteProvider;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TridentOperationContext theContext) {
        propagatorpProvider.open();
    }
    
    @Override
    public void cleanup() {
        propagatorpProvider.close();
    }

    @Override
    public void execute(TridentTuple theTuple, TridentCollector theCollector) {
        Update update = (Update) theTuple.getValueByField("update");
        final boolean shouldProcess = shouldProcess(update);
        Collection<UpdateRow> updateRows = update.getRows();
        Collection<ResultRow> resultRows = new ArrayList<ResultRow>();
        for (UpdateRow updateRow : updateRows) {
            if (shouldProcess) {
                OpenAwareService<UpdatePropagatorContext, ResultRow> propagator =
                        propagatorpProvider.getPropagator(update.getSystemId());
                resultRows.add(propagator.execute(new UpdatePropagatorContext(updateRow, update.getParameters())));
            } else {
                resultRows.add(new ResultRow(updateRow.getId(), ResultRowStatus.SKIPPED, "Update row skipped."));
            }
        }
        theCollector.emit(new Values(new Result(resultRows)));
    }

    private boolean shouldProcess(Update update) {
        return routeProvider.getRoute(update.getSystemId()) == UpdateRoute.EXTERNAL_INTERNAL;
    }
    
}
