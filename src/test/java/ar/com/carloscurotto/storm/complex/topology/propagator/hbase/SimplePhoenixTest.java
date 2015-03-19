package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

/**
 * @author N619614
 */
public class SimplePhoenixTest extends BaseConnectionlessQueryTest {
    
//    static Logger logger = LoggerFactory.getLogger("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.SimplePhoenixTest");

    public static final String LOCALHOST = "localhost";
    public static final String PHOENIX_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR
            + LOCALHOST + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    public static final String PHOENIX_CONNECTIONLESS_JDBC_URL = JDBC_PROTOCOL
            + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS + JDBC_PROTOCOL_TERMINATOR
            + PHOENIX_TEST_DRIVER_URL_PARAM;

    @Test
    public void simpleTest() throws Exception {

        Statement stmt = null;
        ResultSet rset = null;
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");

        Connection con = DriverManager.getConnection(getUrl(), props);
        stmt = con.createStatement();

        ResultSet rs = stmt
                .executeQuery("EXPLAIN "
                        + "create table test (mykey integer not null primary key, mycolumn varchar, fecha date)");
        System.out.println(QueryUtil.getExplainPlan(rs));
        // int up = stmt.executeUpdate("upsert into test values (1,'Hello')");
        // System.out.println(QueryUtil.getExplainPlan(rs));
        // up = stmt.executeUpdate("upsert into test values (2,'World!')");
        // System.out.println(QueryUtil.getExplainPlan(rs));
        // con.commit();
        con
                .createStatement()
                .execute(
                        "create table test (mykey integer not null primary key, mycolumn varchar, fecha date)");
        rset = con.createStatement().executeQuery(
                "EXPLAIN upsert into test values (1,'Hello',to_date('2013-01-01'))");
        System.out.println(QueryUtil.getExplainPlan(rset));
        int count = con.createStatement().executeUpdate(
                "upsert into test values (1,'Hello',to_date('2013-01-01'))");
        System.out.println(count);
        try {
            // TEST.MYKEY may not be null
            count = con.createStatement().executeUpdate(
                    "upsert into test ( mycolumn, fecha ) values ('Hello',to_date('2013-01-01'))");
        } catch (ConstraintViolationException e) {
            System.out.println(e.getMessage());
        }
        try {
            // Type mismatch. DATE and VARCHAR for 2013-01-01
            count = con.createStatement().executeUpdate(
                    "upsert into test values (1, 'Hello','2013-01-01')");
        } catch (TypeMismatchException e) {
            System.out.println(e.getMessage());
        }
        try {
            // Number of columns upserting must match number of values. Numbers of columns: 3. Number of values: 4
            count = con.createStatement().executeUpdate(
                    "upsert into test values (1, 'Hello','2013-01-01', 'hola')");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        System.out.println(count);

        count = con.createStatement().executeUpdate(
                "upsert into test values (2,'Hello',to_date('2013-01-01'))");
        System.out.println(count);

        stmt = con.createStatement();
        rset = stmt.executeQuery("EXPLAIN SELECT * FROM ATABLE LIMIT 1");

        System.out.println(QueryUtil.getExplainPlan(rset));

        stmt.close();
        con.close();
    }

    @Test
    public void TestTradeTable() throws SQLException {

//        logger.debug("Getting the DataSource and the Conncetion from it");
        DataSource theDataSource = new MainConfigForTest().getDataSource(getUrl());

        Connection con = theDataSource.getConnection();

        dropTradeTable(con);
        createTradeTable(con);

//        logger.debug("Calling HBaseUpdatePropagator and see what happens");
        QueryBuilder theQueryBuilder = new QueryBuilder();

        QueryExecutor queryExecutor = new DataSourceQueryExecutor(theDataSource);

        HBaseUpdatePropagator hBaseUpdatePropagator = new HBaseUpdatePropagator(theQueryBuilder,
                queryExecutor);

        hBaseUpdatePropagator.open();

        UpdatePropagatorContext theContext = getUpdatePropagatorContext();

        hBaseUpdatePropagator.propagate(theContext);

//        logger.debug("HBaseUpdatePropagator finished");
        
        showTradeTable(con);

        con.close();
    }

    private void showTradeTable(Connection con) throws SQLException {
        Statement stmt;
        stmt = con.createStatement();

        System.out.println("Hacemos un select de la tabla Trade");
        String sql = TradeTableBuilder.selectAllTradeTable();
        System.out.println("sql: " + sql);
        ResultSet resultSet = stmt.executeQuery(sql);
        con.commit();
        
        while (resultSet.next()) {
            System.out.println(resultSet.getMetaData().toString());
        }
        
        stmt.close();
        
    }

    private void dropTradeTable(Connection con) throws SQLException {

//        logger.debug("Droping the TRADE table first");

        Statement stmt;
        stmt = con.createStatement();

        String sql = TradeTableBuilder.dropTable();
        stmt.execute(sql);
        con.commit();

        stmt.close();

    }

    private void createTradeTable(Connection con) throws SQLException {
//        logger.debug("Creating TRADE table");
        Statement stmt;
        stmt = con.createStatement();

        String sql = TradeTableBuilder.createTable();
        stmt.execute(sql);
        con.commit();

        stmt.close();

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

        UpdatePropagatorContext updatePropagatorContext = new UpdatePropagatorContext("TRADE",
                theRow, theParameters);

        return updatePropagatorContext;
    }

}