package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentOperationContext;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.provider.UpdatePropagatorProvider;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

public abstract class AbstractUpdatePropagatorExecutor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUpdatePropagatorExecutor.class);
    private UpdatePropagatorProvider propagatorProvider;

    public AbstractUpdatePropagatorExecutor(final UpdatePropagatorProvider thePropagatorProvider) {
        Validate.notNull(thePropagatorProvider, "The propagator provider cannot be null");
        propagatorProvider = thePropagatorProvider;
    }

    protected ResultRow process(final Update theUpdate, final UpdateRow theUpdateRow) {
        AbstractUpdatePropagator propagator = propagatorProvider.getPropagator(theUpdate.getSystemId());
        UpdatePropagatorResult updatePropagatorResult = propagator.execute(new UpdatePropagatorContext(theUpdate
                .getTableName(), theUpdateRow, theUpdate.getParameters()));
        ResultRow resultRow = updatePropagatorResult.toResultRow();
        resultRow.setId(theUpdateRow.getId());
        return resultRow;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TridentOperationContext theContext) {
        LOGGER.debug("Opening propagator: " + this.getClass().getName());
        propagatorProvider.open();
    }

    @Override
    public void cleanup() {
        LOGGER.debug("Closing propagator: " + this.getClass().getName());
        propagatorProvider.close();
    }
}
