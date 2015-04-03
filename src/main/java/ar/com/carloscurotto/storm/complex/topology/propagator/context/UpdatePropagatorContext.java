package ar.com.carloscurotto.storm.complex.topology.propagator.context;

import java.util.Map;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;

public class UpdatePropagatorContext {

    private String tableName;
    private UpdateRow row;
    private Map<String, Object> parameters;

    public UpdatePropagatorContext(final String theTableName, final UpdateRow theRow,
            final Map<String, Object> theParameters) {
        Validate.notBlank(theTableName, "The table name can not be blank");
        Validate.notNull(theRow, "The row can not be null");
        Validate.notNull(theParameters, "The parameters can not be null");
        tableName = theTableName;
        row = theRow;
        parameters = theParameters;
    }

    public String getTableName() {
        return tableName;
    }

    public UpdateRow getRow() {
        return row;
    }

    public boolean hasParameters() {
        return !parameters.isEmpty();
    }

    public Object getValueForParameter(final String theParameterName) {
        Validate.notBlank(theParameterName, "The parameter name cannot be blank.");
        return parameters.get(theParameterName);
    }
}
