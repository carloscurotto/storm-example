package ar.com.carloscurotto.storm.complex.topology.propagator;

import java.io.Serializable;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

public abstract class AbstractUpdatePropagator extends OpenAwareService<UpdatePropagatorContext, ResultRow> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    protected abstract ResultRow doExecute(final UpdatePropagatorContext theContext);

}
