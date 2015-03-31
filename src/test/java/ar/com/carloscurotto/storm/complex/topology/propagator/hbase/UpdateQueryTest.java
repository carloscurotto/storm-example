package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

/**
 * @author N619614
 *
 */
public class UpdateQueryTest {

    private QueryBuilder upsertQuery;

    @Test(expected = RuntimeException.class)
    public void parseUpdateEmpty() {
        upsertQuery = new QueryBuilder();
        upsertQuery.build(null);
    }

    @Test(expected = RuntimeException.class)
    public void parseUpdateEmptyRows() {
        upsertQuery = new QueryBuilder();
        upsertQuery.build(null);
    }

    @Test
    public void parseUpdateWithParametersAndRows() {
        upsertQuery = new QueryBuilder();
        String upsertQueryParsed = upsertQuery.build(getUpdateWithColumnsAndParameters());
        //TODO Add timestamp restriction to where condition
        String expected12 = "UPSERT INTO TRADE (column1, column2) SELECT 'value1', 'value2' FROM TRADE WHERE record_no NOT IN ( SELECT record_no FROM TRADE WHERE condition2 = 'conditionValue2' AND condition1 = 'conditionValue1')";
        String expected21 = "UPSERT INTO TRADE (column1, column2) SELECT 'value1', 'value2' FROM TRADE WHERE record_no NOT IN ( SELECT record_no FROM TRADE WHERE condition1 = 'conditionValue1' AND condition2 = 'conditionValue2')";
        boolean condition = upsertQueryParsed.equals(expected12)
                || upsertQueryParsed.equals(expected21);
        assertTrue(condition);
    }

    /**
     * Returns an instance of {@link Update} with 3 Parameters and 1 Rows.
     *
     * @return an instance of {@link Update} with 3 Parameters and 1 Rows. The route type is EXTERNAL_INTERNAL.
     */
    private UpdatePropagatorContext getUpdateWithColumnsAndParameters() {

        HashMap<String, Object> theParameters = new HashMap<String, Object>();
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("condition1", "conditionValue1");
        keyColumns.put("condition2", "conditionValue2");
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("column1", "value1");
        updateColumns.put("column2", "value2");

        UpdateRow theRow = new UpdateRow("theSystemId", keyColumns, updateColumns);

        UpdatePropagatorContext updatePropagatorContext = new UpdatePropagatorContext("TRADE",
                theRow, theParameters);

        return updatePropagatorContext;
    }
}
