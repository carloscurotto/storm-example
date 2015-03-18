package ar.com.carloscurotto.storm.complex.topology.propagator.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.UpdateRow;

public class UpdatePropagatorContext {
    
    private UpdateRow row;
    private Map<String, Object> parameters;
    
    public UpdatePropagatorContext(final UpdateRow theRow, final Map<String, Object> theParameters) {
        Validate.notNull(theRow, "The row can not be null");
        Validate.notNull(theParameters, "The parameters can not be null");
        row = theRow;
        parameters = theParameters;
    }
    
    public UpdatePropagatorContext(final UpdateRow theRow) {
        this(theRow, new HashMap<String, Object>());
    }

    public UpdateRow getRow() {
        return row;
    }
    
    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }
    
}
