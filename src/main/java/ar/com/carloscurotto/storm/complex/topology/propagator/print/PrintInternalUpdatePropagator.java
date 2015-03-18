package ar.com.carloscurotto.storm.complex.topology.propagator.print;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

public class PrintInternalUpdatePropagator extends AbstractUpdatePropagator {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected ResultRow doExecute(UpdatePropagatorContext theContext) {
        System.out.println("Update row received [" + theContext.getRow() + "] on internal update propagator.");
        return new ResultRow(theContext.getRow().getId(), ResultRowStatus.SUCCESS, "Row updated sucessfully.");
    }
}
