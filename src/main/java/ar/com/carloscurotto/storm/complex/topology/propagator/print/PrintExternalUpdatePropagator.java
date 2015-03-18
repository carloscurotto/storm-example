package ar.com.carloscurotto.storm.complex.topology.propagator.print;

import java.io.Serializable;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.ResultRowStatus;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

public class PrintExternalUpdatePropagator extends OpenAwareService<UpdatePropagatorContext, ResultRow> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected ResultRow doExecute(UpdatePropagatorContext theContext) {
        System.out.println("Update row received [" + theContext.getRow() + "] on external update propagator.");
        return new ResultRow(theContext.getRow().getId(), ResultRowStatus.SUCCESS, "Row updated sucessfully.");
    }
}
