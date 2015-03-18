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
 * This class represents each row to be updated. It handles all the required fields to define the unique key and all the
 * columns to be updated with its corresponding values.
 *
 * @author O605461
 */
public class UpdateRow implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * The row id.
     */
    private String id;
    
    /**
     * The column pairs (name, value) that are part of the primary key of the table this update applies to.
     */
    private Map<String, Object> keyColumns;
    
    /**
     * The column pairs (name, value) that needs to be updated for this particular row.
     */
    private Map<String, Object> updateColumns;

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public UpdateRow() {
    }

    /**
     * Creates a row to be updated.
     *
     * @param theId
     *            the row id. It can not be blank.
     * @param theKeyColumns
     *            the column (name, value) pairs that defines the row unique key. It can not be null.
     * @param theUpdateColumns
     *            the column (name, value) paris that defines the columns to be updated for a given update. It can not
     *            be null.
     */
    public UpdateRow(final String theId, final Map<String, Object> theKeyColumns,
            final Map<String, Object> theUpdateColumns) {
        Validate.notBlank(theId, "The id can not be blank");
        Validate.notNull(theKeyColumns, "The key columns cannot be null.");
        Validate.notNull(theUpdateColumns, "The update columns cannot be null.");
        id = theId;
        keyColumns = new HashMap<String, Object>(theKeyColumns);
        updateColumns = new HashMap<String, Object>(theUpdateColumns);
    }

    /**
     * Gets the row id.
     * 
     * @return the row id. It can not be blank.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the names of all the columns that defines the unique key for this update.
     *
     * @return a unmodifiable collection of all the column's names that defines the unique key for this update.
     */
    public Collection<String> getKeyColumnNames() {
        return Collections.unmodifiableSet(keyColumns.keySet());
    }

    /**
     * Gets the value of a key column.
     *
     * @param theKeyColumnName
     *            the key column name. It cannot be blank.
     * @return the value of a key column.
     */
    public Object getKeyColumnValue(final String theKeyColumnName) {
        Validate.notBlank(theKeyColumnName, "The key column name cannot be blank.");
        return keyColumns.get(theKeyColumnName);
    }

    /**
     * Gets the names of all the columns to be updated.
     *
     * @return a unmodifiable collection of all the column's names to be updated.
     */
    public Collection<String> getUpdateColumnNames() {
        return Collections.unmodifiableSet(updateColumns.keySet());
    }

    /**
     * Gets the value of an update column.
     *
     * @param theUpdateColumnName
     *            the update column name. It cannot be blank.
     * @return the value of an update column.
     */
    public Object getUpdateColumnValue(final String theUpdateColumnName) {
        Validate.notBlank(theUpdateColumnName, "The update column name cannot be blank.");
        return updateColumns.get(theUpdateColumnName);
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof UpdateRow) {
            final UpdateRow other = (UpdateRow) object;
            return Objects.equal(id, other.id) && Objects.equal(keyColumns, other.keyColumns)
                    && Objects.equal(updateColumns, other.updateColumns);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, keyColumns, updateColumns);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("keyColumns", keyColumns)
                .add("updateColumns", updateColumns).toString();
    }
    
}
