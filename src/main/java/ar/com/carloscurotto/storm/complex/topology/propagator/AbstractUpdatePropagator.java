package ar.com.carloscurotto.storm.complex.topology.propagator;

import java.io.Serializable;

import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

public abstract class AbstractUpdatePropagator extends
        OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> implements Serializable {

    private static final long serialVersionUID = 1L;
    
    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }    

    @Override
    protected abstract UpdatePropagatorResult doExecute(final UpdatePropagatorContext theContext);

}
