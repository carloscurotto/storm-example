package ar.com.carloscurotto.storm.complex.topology.propagator.provider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;
import ar.com.carloscurotto.storm.complex.service.OpenAwarePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

import com.google.common.base.Preconditions;

/**
 * Provides the propagator that each update should use inside our topology.
 *
 * @author O605461
 */
public class UpdatePropagatorProvider extends OpenAwareBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult>> propagators;

    public UpdatePropagatorProvider(
            final Map<String, OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult>> thePropagators) {
        Validate.notNull(thePropagators, "The propagators can not be null.");
        propagators = new HashMap<String, OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult>>(
                thePropagators);
    }

    @Override
    public void doOpen() {
        for (OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> propagator : propagators.values()) {
            propagator.open();
        }
    }

    @Override
    public void doClose() {
        for (OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> propagator : propagators.values()) {
            propagator.close();
        }
    }

    public OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> getPropagator(final String theSystemId) {
        Validate.notBlank(theSystemId, "The system id can not be blank.");
        Preconditions.checkState(isOpen(), "The provider is not open. Please, open it first.");
        OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> propagator = propagators.get(theSystemId);
        Preconditions.checkState(propagator != null, "Can not find a propagator for the system id [" + theSystemId
                + "]. Please, configure the propagator properly.");
        return propagator;
    }

}
