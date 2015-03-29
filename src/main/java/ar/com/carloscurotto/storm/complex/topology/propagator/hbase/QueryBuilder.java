package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.springframework.util.StringUtils.collectionToDelimitedString;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

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
        upsertQuery.append(createUpsertClause(theTableName, theUpdateRow));
//        upsertQuery.append(" WHERE ").append(createWhereClause(theUpdateRow));
        return upsertQuery.toString();
    }

    //TODO
    /* This is just to guide writing the Query
     * Delete it when not more necessary
     * UPSERT INTO tableTarget(col1, col2) SELECT col3, col4 FROM tableSource WHERE col5 < 100
     */
    
    private String createSelectStatement(String theTableName, List<String> theColumnNames, List<String> theColumnValues, Set<Entry<String, Object>> theKeyColumnEntries) {
	StringBuilder builder = new StringBuilder();
	builder.append(" SELECT ").append(collectionToDelimitedString(theColumnNames, ", ")).append(" FROM ").append(theTableName);
	builder.append(" WHERE ").append(createAndSeparatedCondition(theKeyColumnEntries));
	return builder.toString();
    }

    private String createAndSeparatedCondition(Set<Entry<String,Object>> theKeyColumnEntries) {
        List<String> keyValueConditions = new ArrayList<String>();
        for (Entry<String, Object> keyColumnEntry : theKeyColumnEntries) {
            keyValueConditions.add(keyColumnEntry.getKey() + " = " + valueFormatter(keyColumnEntry.getValue()));
        }
        return collectionToDelimitedString(keyValueConditions, " AND ");
    }

    private String createUpsertClause(String theTableName, UpdateRow theUpdateRow) {
        List<String> columnNames = new ArrayList<String>();
        List<String> columnValues = new ArrayList<String>();
        for (Entry<String, Object> updateColumnEntry : theUpdateRow.getUpdateColumnEntries()) {
            columnNames.add(updateColumnEntry.getKey());
            Object value = updateColumnEntry.getValue();
            columnValues.add(valueFormatter(value));
        }
        StringBuilder builder = new StringBuilder();
        builder.append("UPSERT INTO ").append(theTableName).append(" ");
        builder.append("(").append(collectionToDelimitedString(columnNames, ", ")).append(")");
//        builder.append(createSelectStatement(theTableName, columnNames, columnValues, theUpdateRow.getKeyColumnEntries()));
        builder.append(createValueClause(columnValues));
        return builder.toString();
    }
    
    private String createValueClause(List<String> theColumnValues) {
	StringBuilder builder = new StringBuilder();
	builder.append(" VALUES ").append("(").append(collectionToDelimitedString(theColumnValues, ", ")).append(")");
	return builder.toString();
    }

    private String valueFormatter(Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof Date) {
            return "to_date('" + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(value) + "')";
        } else { 
            return "'" + value.toString() + "'";
        }
    }
}
