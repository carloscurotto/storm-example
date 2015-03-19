package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.springframework.util.StringUtils.collectionToDelimitedString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

/**
 * This class is in charge of building the query from the {@link Collection<UpsertRow>} used to insert/update data into
 * the database.
 *
 * @author N619614
 *
 */
public class QueryBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Builds the query from the {@link UpdatePropagatorContext}.
     *
     * @param theUpdatePropagatorContext
     *            the UpdatePropagatorContext where this builder will build the query from. It can not be null.
     * @return the query as a String. It is never null nor empty.
     */
    public String build(final UpdatePropagatorContext theUpdatePropagatorContext) {
        Validate.notNull(theUpdatePropagatorContext, "The update can not be null");
        String query = generateUpsertQuery(theUpdatePropagatorContext.getTableName(), theUpdatePropagatorContext.getRow());
        return query;
    }

    //TODO Change the where part and use the primary key to check if a row already exists
    private String generateUpsertQuery(final String theTableName, final UpdateRow theUpdateRow) {
        Validate.notBlank(theTableName, "The table name can not be blank.");
        Validate.notNull(theUpdateRow, "The update row can not be null.");
        StringBuilder upsertQuery = new StringBuilder();
        upsertQuery.append("UPSERT INTO ").append(theTableName).append(" ").append(createUpsertClause(theUpdateRow));
        upsertQuery.append(" WHERE ").append(createWhereClause(theUpdateRow));
        return upsertQuery.toString();
    }

    private String createWhereClause(UpdateRow updateRow) {
        List<String> keyValueConditions = new ArrayList<String>();
        for (Entry<String, Object> keyColumnEntry : updateRow.getKeyColumnEntries()) {
            keyValueConditions.add(keyColumnEntry.getKey() + " = '" + keyColumnEntry.getValue() + "'");
        }
        return collectionToDelimitedString(keyValueConditions, " AND ");
    }

    private String createUpsertClause(UpdateRow updateRow) {
        List<String> columnNames = new ArrayList<String>();
        List<Object> columnValues = new ArrayList<Object>();
        for (Entry<String, Object> updateColumnEntry : updateRow.getUpdateColumnEntries()) {
            columnNames.add(updateColumnEntry.getKey());
            Object value = updateColumnEntry.getValue();
            //TODO the condition below doesnt work
            columnValues.add((value instanceof Number || value instanceof Boolean)?value:"'" + value.toString() + "'");
        }
        StringBuilder builder = new StringBuilder();
        builder.append("(").append(collectionToDelimitedString(columnNames, ", ")).append(")");
        builder.append(" VALUES ").append("(")
                .append(collectionToDelimitedString(columnValues, ", ")).append(")");
        return builder.toString();
    }
}
