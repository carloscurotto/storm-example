package ar.com.carloscurotto.storm.complex.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class Result {

    private Map<String, ResultRow> rows = new HashMap<String, ResultRow>();

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public Result() {}
    
    public Result(final Collection<ResultRow> theRows) {
        Preconditions.checkArgument(theRows != null, "The rows can not be null.");
        for (ResultRow theRow : theRows) {
            rows.put(theRow.getId(), theRow);
        }
    }
    
    public Collection<ResultRow> getRows() {
        return Collections.unmodifiableCollection(rows.values());
    }
    
    public ResultRow getRow(final String theId) {
        Validate.notBlank(theId, "The id can not be blank");
        return rows.get(theId);
    }
    
    @Override
    public boolean equals(final Object object) {
        if (object instanceof Result) {
            final Result other = (Result) object;
            return Objects.equal(rows, other.rows);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rows);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("rows", rows).toString();
    }
    
}
