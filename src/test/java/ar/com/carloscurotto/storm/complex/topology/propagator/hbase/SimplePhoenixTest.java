package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Ignore;
import org.junit.Test;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;

/**
 * @author N619614
 */
public class SimplePhoenixTest extends BaseConnectionlessQueryTest {
    public static final String LOCALHOST = "localhost";
    public static final String PHOENIX_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR
            + LOCALHOST + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    public static final String PHOENIX_CONNECTIONLESS_JDBC_URL = JDBC_PROTOCOL
            + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS + JDBC_PROTOCOL_TERMINATOR
            + PHOENIX_TEST_DRIVER_URL_PARAM;

    @Ignore
    @Test
    public void simpleTest() throws Exception {

        Statement stmt = null;
        ResultSet rset = null;
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
        
        Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:42100", props);
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
        con.commit();
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
        
        createTradeTable(con);
        
        count = con.createStatement().executeUpdate(
        "UPSERT INTO TRADE (Postage, CounterpartyDescription, TradingCurrency, OverdueDays, Origin, AGE_BUCKET, record_no, MISSING_SSI_IND, BROKER_SHRT_NME, HIGH_VALUE_QTY_IND, FirmAccount, RAG, OpenConsideration, ENTITY_ID, RrNo, TGID, Levy, TradeType, SettlementTypeDescription, Price, AUTOFX_IND, OpenConsiderationUSD, PrimaryQuantity, TradeGroupType, AdpInstrumentRefDesc, AdpInstrumentRef, BROKER_COVERAGE_NAME, MARKET, Stamp, instNumber, ProcStatus, Tariff, ReportTradeType, TRSTradeRef, PENDING_UPDATE, StorageStatus, FirmAccountDescription, CompanyGlossReference, TimeStamp, CpartyExternalReference, ValueDate, Vat, Commission, SettleQuantity, TradeCategoryDesc, OperationType, TRADE_TYPE_DESC, SpecialInstr4, SpecialInstr3, BsNarrative, OpenQuantity, BankStatus, PrincipalTradeCurrency, RELATIONSHIP_ID, ExchangeRateIndicator, KEY_CLIENT_IND, CounterpartyReference, BUYIN, BankStatusCategory, Exposure, BUSINESS, GrossCredit, REGION, TradeVersion, tradeNo, SettlementDiv, ExchangeRate, SettleStatus, BankStatusDesciption, ClearanceCode, NetConsideration, PrincipalSettlingCurrency, UserName, SEMEID, OperationTypeDescription, LinkType, SettleCode, ASSIGN_TO, TradeStatus, AccruedInterest, ActualSettleDate, RPT_MKT_REGION, RelationshipOrEntityId, TradeDate, AGE, SIDE, IsinReference, FAMILY_SNAME, TradeReference, SettlementCurrency, IntNarrative, ManualStatusInd, service, ParentTradeReference, SpecialInstr1, SpecialInstr2, FinanceRate) VALUES (123456, 'Valor', 'Valor', 123456, 'Valor', 'Valor', 1, true, 'Valor', true, 'Valor', 'Valor', 123456, 'Valor', 'Valor', 'Valor', 123456, 'Valor', 'Valor', 123456, true, 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 123456, 'Valor', 123456, 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', to_date('2015-03-19 16:02:30'), 123456, 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 'Valor', 123456, 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 123456, 'Valor', 123456, 'Valor', 123456, 123456, 'Valor', 123456, 'Valor', 'Valor', 'Valor', 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 'Valor', 123456)");
        
        stmt = con.createStatement();
        rset = stmt.executeQuery("EXPLAIN SELECT * FROM TRADE LIMIT 1");

        System.out.println(QueryUtil.getExplainPlan(rset));


        stmt.close();
        con.close();
    }
    
    @Test
    public void TestTradeTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        
        Statement stmt = null;
        ResultSet rset = null;
        

//        logger.debug("Getting the DataSource and the Conncetion from it");
//        DataSource theDataSource = new MainConfigForTest().getDataSource(getUrl());
//        Connection con = theDataSource.getConnection();
        
        DriverManager.registerDriver((Driver) Class.forName("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.PhoenixDriverNonCommit").newInstance());
        //Connection con = DriverManager.getConnection("jdbc:phoenix:none;test=true");
        //Connection con = DriverManager.getConnection("jdbc:losi:none");
//        Connection con = DriverManager.getConnection("jdbc:losiiii:none;test=true");
    
        Properties props = new Properties();
        props.setProperty(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
        Connection con = DriverManager.getConnection(getUrl(), props).unwrap(NoCommitConnection.class);

        //dropTradeTable(con);
        
        createTradeTable(con);
        
        int count = con.createStatement().executeUpdate(
                "UPSERT INTO TRADE (Postage, CounterpartyDescription, TradingCurrency, OverdueDays, Origin, AGE_BUCKET, record_no, MISSING_SSI_IND, BROKER_SHRT_NME, HIGH_VALUE_QTY_IND, FirmAccount, RAG, OpenConsideration, ENTITY_ID, RrNo, TGID, Levy, TradeType, SettlementTypeDescription, Price, AUTOFX_IND, OpenConsiderationUSD, PrimaryQuantity, TradeGroupType, AdpInstrumentRefDesc, AdpInstrumentRef, BROKER_COVERAGE_NAME, MARKET, Stamp, instNumber, ProcStatus, Tariff, ReportTradeType, TRSTradeRef, PENDING_UPDATE, StorageStatus, FirmAccountDescription, CompanyGlossReference, TimeStamp, CpartyExternalReference, ValueDate, Vat, Commission, SettleQuantity, TradeCategoryDesc, OperationType, TRADE_TYPE_DESC, SpecialInstr4, SpecialInstr3, BsNarrative, OpenQuantity, BankStatus, PrincipalTradeCurrency, RELATIONSHIP_ID, ExchangeRateIndicator, KEY_CLIENT_IND, CounterpartyReference, BUYIN, BankStatusCategory, Exposure, BUSINESS, GrossCredit, REGION, TradeVersion, tradeNo, SettlementDiv, ExchangeRate, SettleStatus, BankStatusDesciption, ClearanceCode, NetConsideration, PrincipalSettlingCurrency, UserName, SEMEID, OperationTypeDescription, LinkType, SettleCode, ASSIGN_TO, TradeStatus, AccruedInterest, ActualSettleDate, RPT_MKT_REGION, RelationshipOrEntityId, TradeDate, AGE, SIDE, IsinReference, FAMILY_SNAME, TradeReference, SettlementCurrency, IntNarrative, ManualStatusInd, service, ParentTradeReference, SpecialInstr1, SpecialInstr2, FinanceRate) VALUES (123456, 'Valor', 'Valor', 123456, 'Valor', 'Valor', 1, true, 'Valor', true, 'Valor', 'Valor', 123456, 'Valor', 'Valor', 'Valor', 123456, 'Valor', 'Valor', 123456, true, 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 123456, 'Valor', 123456, 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', to_date('2015-03-19 16:02:30'), 123456, 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 'Valor', 123456, 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 123456, 'Valor', 123456, 'Valor', 123456, 123456, 'Valor', 123456, 'Valor', 'Valor', 'Valor', 123456, 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 123456, 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', 'Valor', true, 'Valor', 'Valor', 'Valor', 'Valor', 123456)");
        System.out.println(count);
    
        con.commit();
        
                
//                stmt = con.createStatement();
//                rset = stmt.executeQuery("EXPLAIN SELECT * FROM TRADE LIMIT 1");
//
//                System.out.println(count);
//                System.out.println(QueryUtil.getExplainPlan(rset));
                
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
        
        int i = 1;
        while (resultSet.next()) {
            System.out.println(i++ + ": " + resultSet.getMetaData().toString());
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