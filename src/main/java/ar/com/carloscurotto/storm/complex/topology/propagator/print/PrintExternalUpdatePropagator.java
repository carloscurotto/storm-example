package ar.com.carloscurotto.storm.complex.topology.propagator.print;

import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

public class PrintExternalUpdatePropagator extends AbstractUpdatePropagator {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected UpdatePropagatorResult doExecute(UpdatePropagatorContext theContext) {
        System.out.println("Update row received [" + theContext.getRow() + "] on external update propagator.");
        return UpdatePropagatorResult.createSuccess("Row updated sucessfully.");
    }

}
