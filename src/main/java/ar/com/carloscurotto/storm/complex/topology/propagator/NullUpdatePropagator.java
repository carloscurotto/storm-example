package ar.com.carloscurotto.storm.complex.topology.propagator;

import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

public class NullUpdatePropagator extends AbstractUpdatePropagator {

    private static final long serialVersionUID = 1L;

    @Override
    protected UpdatePropagatorResult doExecute(UpdatePropagatorContext theContext) {
        return UpdatePropagatorResult.createSuccess("Row updated sucessfully.");
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

}
