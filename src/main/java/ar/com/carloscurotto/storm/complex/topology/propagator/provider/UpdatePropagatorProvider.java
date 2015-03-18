package ar.com.carloscurotto.storm.complex.topology.propagator.provider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.Closeable;
import ar.com.carloscurotto.storm.complex.service.OpenAwareService;
import ar.com.carloscurotto.storm.complex.service.Openable;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

import com.google.common.base.Preconditions;

/**
 * Provides the propagator that each update should use inside our topology.
 *
 * @author O605461
 */
public class UpdatePropagatorProvider implements Openable, Closeable, Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult>> propagators;

    private boolean isOpen;

    public UpdatePropagatorProvider(
            final Map<String, OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult>> thePropagators) {
        Validate.notNull(thePropagators, "The propagators can not be null.");
        propagators = new HashMap<String, OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult>>(
                thePropagators);
    }

    @Override
    public void open() {
        Preconditions.checkState(!isOpen(), "The service is already opened.");
        for (OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> propagator : propagators
                .values()) {
            propagator.open();
        }
        isOpen = true;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() {
        for (OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> propagator : propagators
                .values()) {
            propagator.close();
        }
        isOpen = false;
    }

    public OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> getPropagator(
            final String theSystemId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(theSystemId),
                "The system id can not be blank.");
        Preconditions.checkState(isOpen(), "The provider is not open. Please, open it first.");
        OpenAwareService<UpdatePropagatorContext, UpdatePropagatorResult> propagator = propagators
                .get(theSystemId);
        Preconditions.checkState(propagator != null,
                "Can not find a propagator for the system id [" + theSystemId
                        + "]. Please, configure the propagator properly.");
        return propagator;
    }

}
