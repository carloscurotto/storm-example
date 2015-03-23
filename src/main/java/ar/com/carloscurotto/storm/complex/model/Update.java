package ar.com.carloscurotto.storm.complex.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Represents each update that we are going to receive containing all the parameters to execute it.
 *
 * @author O605461
 */
public class Update implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * The id that identifies this update.
     */
    private String id;
    /**
     * This id tells to which system this particular update applies to.
     */
    private String systemId;
    /**
     * The name of the table this update applies to.
     */
    private String tableName;
    /**
     * The parameters for this particular update.
     */
    private Map<String, Object> parameters = new HashMap<String, Object>();
    /**
     * The updated rows.
     */
    private Map<String, UpdateRow> rows = new HashMap<String, UpdateRow>();

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public Update() {
    }

    /**
     * Constructor.
     *
     * @param theId the id that identifies this update.
     * @param theSystemId
     *            the systems's id for this particular update. It can not be blank.
     * @param theTableName
     *            the table name where this update apply to. It can not be blank.
     * @param theParameters
     *            the parameters for this particular update. It can not be null.
     * @param theRows
     *            the rows updated. It can not be null nor empty.
     */
    public Update(final String theId, final String theSystemId, final String theTableName,
            final Map<String, Object> theParameters, final Collection<UpdateRow> theRows) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notBlank(theSystemId, "The system id can not be blank.");
        Validate.notBlank(theTableName, "The table name can not be blank.");
        Validate.notNull(theParameters, "The parameters can not be null.");
        Validate.notNull(theRows, "The rows can not be null.");
        id = theId;
        systemId = theSystemId;
        tableName = theTableName;
        parameters.putAll(theParameters);
        for (UpdateRow theRow : theRows) {
            rows.put(theRow.getId(), theRow);
        }
    }
    
    /**
     * Gets the update's id.
     * 
     * @return the update's id.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the update's system id.
     *
     * @return the update's system id.
     */
    public String getSystemId() {
        return systemId;
    }

    /**
     * Returns the update table name.
     *
     * @return the update table name. It is never null nor empty.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the names of all the parameters for this particular update.
     *
     * @return the names of all the parameters for this particular update.
     */
    public Collection<String> getParameterNames() {
        return Collections.unmodifiableCollection(parameters.keySet());
    }

    /**
     * Gets the value for the given parameter name or null if we do not have a parameter with the given name.
     *
     * @param parameterName
     *            the parameter name.
     * @return the value for the given parameter name.
     */
    public Object getParameterValue(final String parameterName) {
        Validate.notBlank(parameterName, "The parameter name cannot be blank.");
        return parameters.get(parameterName);
    }

    /**
     * Gets the current quantity of parameters.
     *
     * @return the current quantity of parameters.
     */
    public int getParametersQuantity() {
        return parameters.size();
    }

    /**
     * Gets all the update parameters.
     *
     * @return the update parameters. It can not be null.
     */
    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }

    /**
     * Gets the rows for this particular update.
     *
     * @return the rows for this particular update.
     */
    public Collection<UpdateRow> getRows() {
        return Collections.unmodifiableCollection(rows.values());
    }
    
    /**
     * Gets all the ids of every row.
     *
     * @return the ids of every row. It is never null nor empty.
     */
    public Collection<String> getRowsId() {
        return Collections.unmodifiableCollection(rows.keySet());
    }    

    /**
     * Gets the row associated with the given id.
     *
     * @param theId
     *            the given row id. It can not be blank.
     * @return the row associated with the given row id or null if there is no row associated with the given id.
     */
    public UpdateRow getRow(final String theId) {
        Validate.notBlank(theId, "The id can not be blank");
        return rows.get(theId);
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof Update) {
            final Update other = (Update) object;
            return Objects.equal(id, other.id) && Objects.equal(systemId, other.systemId)
                    && Objects.equal(tableName, other.tableName) && Objects.equal(parameters, other.parameters)
                    && Objects.equal(rows, other.rows);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, systemId, tableName, parameters, rows);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("systemId", systemId).add("tableName", tableName)
                .add("parameters", parameters).add("rows", rows).toString();
    }
}
