package ar.com.carloscurotto.storm.complex.topology.propagator;

import java.io.Serializable;

import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * {@inheritDoc}
 * 
 * @author O605461, W506376
 */
public abstract class AbstractUpdatePropagator extends OpenAwareBean<UpdatePropagatorContext, UpdatePropagatorResult>
        implements Serializable {

    private static final long serialVersionUID = 1L;

}
