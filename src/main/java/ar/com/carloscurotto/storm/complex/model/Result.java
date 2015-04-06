package ar.com.carloscurotto.storm.complex.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class Result implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private Map<String, ResultRow> rows = new HashMap<String, ResultRow>();

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public Result() {}

    public Result(final String theId, final Collection<ResultRow> theRows) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notNull(theRows, "The rows can not be null.");
        Validate.notEmpty(theRows, "The rows can not be empty.");
        id = theId;
        for (ResultRow theRow : theRows) {
            rows.put(theRow.getId(), theRow);
        }
    }

    public String getId() {
        return id;
    }

    public Collection<ResultRow> getRows() {
        return Collections.unmodifiableCollection(rows.values());
    }

    public ResultRow getRow(final String theId) {
        Validate.notBlank(theId, "The id can not be blank.");
        ResultRow row = rows.get(theId);
        Preconditions.checkState(row != null, "Can not find a row with id [" + theId + "].");
        return row;
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof Result) {
            final Result other = (Result) object;
            return Objects.equal(id, other.id) && Objects.equal(rows, other.rows);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, rows);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("rows", rows).toString();
    }

}
