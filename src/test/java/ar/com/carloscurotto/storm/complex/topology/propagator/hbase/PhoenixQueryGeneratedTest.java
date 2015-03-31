package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

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

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class PhoenixQueryGeneratedTest extends BaseConnectionlessQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixQueryGeneratedTest.class);

    private static final String URL_PHOENIX_DRIVER_TEST = "jdbc:losiiii:none;test=true";
    
    private static ComboPooledDataSource dataSource;
    
    @BeforeClass
    public static void setUp() {
        dataSource = createDataSource();
        createTradeTable();
    }

    private static void createTradeTable() {
        Connection connection = null;
        try {
            String sql = TradeTableBuilder.createTable();
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
            connection.commit();
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("Could not create table : ", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    private static ComboPooledDataSource createDataSource() {
        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        try {
            DriverManager.registerDriver((Driver) Class.forName(PhoenixDriverNonCommit.class.getCanonicalName()).newInstance());
            comboPooledDataSource.setDriverClass(PhoenixDriverNonCommit.class.getCanonicalName());
        } catch (PropertyVetoException | InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Couldn't load driver class", e);
        }
        comboPooledDataSource.setJdbcUrl(URL_PHOENIX_DRIVER_TEST);
        comboPooledDataSource.setInitialPoolSize(2);
        comboPooledDataSource.setMinPoolSize(2);
        comboPooledDataSource.setMaxPoolSize(2);
        comboPooledDataSource.setAcquireIncrement(1);
        comboPooledDataSource.setConnectionCustomizerClassName(AutocommitFalseConnectionCustomizer.class.getCanonicalName());
        return comboPooledDataSource;
    }

    @AfterClass
    public static void tearDown() {
        dropTradeTable();
        destroyDataSource();
    }

    private static void dropTradeTable() {
        Connection connection = null;
        try {
            String sql = TradeTableBuilder.dropTable();
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
            connection.commit();
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("Could not drop table : ", e);
        } finally {
            DbUtils.closeQuietly(connection);
            dataSource.close();
        }
    }

    private static void destroyDataSource() {
        dataSource.close();
        dataSource = null;
    }
    
    @Test
    public void queryResultSuccess() throws SQLException, ParseException {
        QueryExecutor theQueryExecutor = getMockQueryExecutor();
        QueryBuilder theQueryBuilder = new QueryBuilder();
        HBaseUpdatePropagator hBaseUpdatePropagator = new HBaseUpdatePropagator(theQueryBuilder,
                theQueryExecutor);
        hBaseUpdatePropagator.open();
        UpdatePropagatorContext theContext = getUpdatePropagatorContext();
        UpdatePropagatorResult updatePropagatorResult = hBaseUpdatePropagator.propagate(theContext);
        assertThat(updatePropagatorResult.getStatus(), equalTo(ResultStatus.SUCCESS));
    }

    @SuppressWarnings("unused")
    private QueryExecutor getQueryExecutor() {
        return new DataSourceQueryExecutor(dataSource);
    }

    private QueryExecutor getMockQueryExecutor() {
        return new PrintQueryExecutor();
    }

    private UpdatePropagatorContext getUpdatePropagatorContext() throws ParseException {
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("record_no", 1);
        keyColumns.put("TimeStamp",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2015-03-19 16:02:30"));

        Map<String, Object> updateColumns = new HashMap<String, Object>();
        String[] columnNames = TradeTableBuilder.createColumnNames();
        Object[] columnValues = TradeTableBuilder.createColumnValues();
        for (int i = 0; i < columnNames.length; i++) {
            updateColumns.put(columnNames[i], columnValues[i]);
        }

        HashMap<String, Object> theParameters = new HashMap<String, Object>();
        UpdateRow theRow = new UpdateRow("theSystemId", keyColumns, updateColumns);

        return new UpdatePropagatorContext("TRADE", theRow, theParameters);
    }

}
