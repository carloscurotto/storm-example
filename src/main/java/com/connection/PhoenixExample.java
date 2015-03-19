package com.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.DataSourceQueryExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.HBaseUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.MainConfigForTest;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.QueryBuilder;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.QueryExecutor;
import ar.com.carloscurotto.storm.complex.topology.propagator.hbase.TradeTableBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixExample {
    
    static Logger logger = LoggerFactory.getLogger("com.connection.PhoenixExample");

    public static void main(String[] args) throws SQLException {
        
        logger.debug("Starting the Phoenix-Hbase test");

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your PhoenixDriver");
            e.printStackTrace();
            return;
        }
        Statement stmt = null;
        ResultSet rset = null;

        String conString = buildURL();

        Properties props = getPropertiesConfigured();

//        Connection con = new MainConfigForTest().getDataSource().getConnection();
//        Connection con = DriverManager.getConnection(conString);
        // Connection con = DriverManager.getConnection(conString, props);

//        SimpleTest(props, conString);
        
        TestTradeTable();

    }
    
    public static void TestTradeTable() throws SQLException {
        
        logger.debug("Getting the DataSource and the Conncetion from it");
        DataSource theDataSource = null;//new MainConfigForTest().getDataSource();
        
        Connection con = theDataSource.getConnection();
        
        dropTradeTable(con);
        createTradeTable(con);
        
        logger.debug("Calling HBaseUpdatePropagator and see what happens");
        QueryBuilder theQueryBuilder = new QueryBuilder();
        
        QueryExecutor queryExecutor = new DataSourceQueryExecutor(theDataSource);
        
        HBaseUpdatePropagator hBaseUpdatePropagator = new HBaseUpdatePropagator(theQueryBuilder, queryExecutor);
        
        hBaseUpdatePropagator.open();
        
        UpdatePropagatorContext theContext = getUpdatePropagatorContext();
        
        hBaseUpdatePropagator.propagate(theContext );
        
        logger.debug("HBaseUpdatePropagator finished");
        
        con.close();
    }

    private static void dropTradeTable(Connection con) throws SQLException {
        
        logger.debug("Droping the TRADE table first");
        
        Statement stmt;
        ResultSet rset;
        stmt = con.createStatement();
        
        String sql = TradeTableBuilder.dropTable();
        stmt.execute(sql);
        con.commit();
        
        stmt.close();
        
    }

    private static void createTradeTable(Connection con) throws SQLException {
        logger.debug("Creating TRADE table");
        Statement stmt;
        ResultSet rset;
        stmt = con.createStatement();
        
        String sql = TradeTableBuilder.createTable();
        stmt.execute(sql);
        con.commit();
        
        stmt.close();
        
        
    }

    private static void SimpleTest(Properties props, String conString) throws SQLException {
        
        Connection con = DriverManager.getConnection(conString);
        
        Statement stmt;
        ResultSet rset;
        stmt = con.createStatement();

        stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
        stmt.executeUpdate("upsert into test values (1,'Hello')");
        stmt.executeUpdate("upsert into test values (2,'World!')");
        con.commit();

        PreparedStatement statement = con.prepareStatement("select * from test");
        rset = statement.executeQuery();
        while (rset.next()) {
            System.out.println(rset.getString("mycolumn"));
        }
        statement.close();
        
        con.close();
    }

    private static String buildURL() {
        // String conString = "jdbc:phoenix:192.168.56.101:2181";
        // String conString = "jdbc:phoenix:127.0.0.1:2181";
        String conString = "jdbc:phoenix:localhost:2181:/hbase";
        // String conString = "jdbc:phoenix:sandbox.hortonworks.com";
        return conString;
    }

    private static Properties getPropertiesConfigured() {
        Properties props = new Properties();

        props.setProperty("hbase.client.retries.number", "1");
        props.setProperty("zookeeper.recovery.retry", "1");

        // props.setProperty("user","fred");

        props.setProperty("hbase.master", "master" + ":" + "60000");
        props.setProperty("zookeeper.znode.parent", "/hbase-unsecure");
        props.setProperty("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        props.setProperty("hbase.zookeeper.property.clientPort", "2181");
        return props;
    }
    
    private static UpdatePropagatorContext getUpdatePropagatorContext() {
        HashMap<String, Object> theParameters = new HashMap<String, Object>();
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        
        keyColumns.put("record_no", 1);
//        keyColumns.put("condition1", "conditionValue1");
//        keyColumns.put("condition2", "conditionValue2");
        
        String[] columnNames = TradeTableBuilder.createColumnNames();
        Object[] columnValues = TradeTableBuilder.createColumnValues();
        
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        for (int i=0; i < columnNames.length; i++) {
            updateColumns.put(columnNames[i], columnValues[i]);
        }
        
//        updateColumns.put("column2", "value2");

        UpdateRow theRow = new UpdateRow("theSystemId", keyColumns, updateColumns);

        UpdatePropagatorContext updatePropagatorContext = new UpdatePropagatorContext("TRADE",
                theRow, theParameters);
        
        return updatePropagatorContext;
    }

}
