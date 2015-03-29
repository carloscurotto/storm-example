package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.ResultStatus;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

public class PhoenixQueryGeneratedTest extends BaseConnectionlessQueryTest {

    static Logger logger = LoggerFactory.getLogger("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.PhoenixQueryGeneratedTest");

    private static final String URL_PHOENIX_DRIVER_TEST = "jdbc:losiiii:none;test=true";

    @Test
    public void queryResultSuccess() throws SQLException {
	DataSource theDataSource = new MainConfigForTest().getDataSource(URL_PHOENIX_DRIVER_TEST);
	QueryBuilder theQueryBuilder = new QueryBuilder();
	QueryExecutor queryExecutor = new DataSourceQueryExecutor(theDataSource);
	HBaseUpdatePropagator hBaseUpdatePropagator = new HBaseUpdatePropagator(theQueryBuilder, queryExecutor);
	hBaseUpdatePropagator.open();
	UpdatePropagatorContext theContext = getUpdatePropagatorContext();
	UpdatePropagatorResult updatePropagatorResult = hBaseUpdatePropagator.execute(theContext);
	assertThat(updatePropagatorResult.getStatus(), equalTo(ResultStatus.SUCCESS));
    }

    private UpdatePropagatorContext getUpdatePropagatorContext() {
	HashMap<String, Object> theParameters = new HashMap<String, Object>();
	Map<String, Object> keyColumns = new HashMap<String, Object>();

	keyColumns.put("record_no", 1);
	// keyColumns.put("condition1", "conditionValue1");
	// keyColumns.put("condition2", "conditionValue2");

	String[] columnNames = TradeTableBuilder.createColumnNames();
	Object[] columnValues = TradeTableBuilder.createColumnValues();

	Map<String, Object> updateColumns = new HashMap<String, Object>();
	for (int i = 0; i < columnNames.length; i++) {
	    updateColumns.put(columnNames[i], columnValues[i]);
	}

	// updateColumns.put("column2", "value2");

	UpdateRow theRow = new UpdateRow("theSystemId", keyColumns, updateColumns);

	UpdatePropagatorContext updatePropagatorContext = new UpdatePropagatorContext("TRADE", theRow, theParameters);

	return updatePropagatorContext;
    }

    @BeforeClass
    public static void setUp() {
	Connection connection = null;
	try {
	    DataSource theDataSource = new MainConfigForTest().getDataSource(URL_PHOENIX_DRIVER_TEST);
	    connection = theDataSource.getConnection();
	    createTradeTable(connection);
	} catch (SQLException e) {
	    logger.error("Could not create table : ", e);
	} finally {
	    DbUtils.closeQuietly(connection);
	}
    }

    private static void createTradeTable(Connection connection) throws SQLException {
	Statement statement = connection.createStatement();
	String sql = TradeTableBuilder.createTable();
	statement.execute(sql);
	connection.commit();
	statement.close();
    }

    @AfterClass
    public static void setDown() {
	Connection connection = null;
	try {
	    DataSource theDataSource = new MainConfigForTest().getDataSource(URL_PHOENIX_DRIVER_TEST);
	    connection = theDataSource.getConnection();
	    dropTradeTable(connection);
	} catch (SQLException e) {
	    logger.error("Could not drop table : ", e);
	} finally {
	    DbUtils.closeQuietly(connection);
	}

    }

    private static void dropTradeTable(Connection connection) throws SQLException {
	Statement statement = connection.createStatement();
	String sql = TradeTableBuilder.dropTable();
	statement.execute(sql);
	connection.commit();
	statement.close();
    }
}
