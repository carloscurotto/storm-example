package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TradeTableBuilder {

    public static String dropTable() {
        return "DROP TABLE IF EXISTS TRADE";
    }

    public static String createTable() {
        return "CREATE TABLE IF NOT EXISTS TRADE(record_no UNSIGNED_LONG PRIMARY KEY, FirmAccount VARCHAR,FirmAccountDescription VARCHAR,AdpInstrumentRef VARCHAR,AdpInstrumentRefDesc VARCHAR,TradingCurrency VARCHAR,IsinReference VARCHAR,PrimaryQuantity FLOAT,SettleQuantity FLOAT,CounterpartyReference VARCHAR,CounterpartyDescription VARCHAR,TradeType VARCHAR,OperationType VARCHAR,OperationTypeDescription VARCHAR,NetConsideration FLOAT,PrincipalTradeCurrency FLOAT,PrincipalSettlingCurrency FLOAT,AccruedInterest FLOAT,TradeDate VARCHAR,ValueDate DATE,ActualSettleDate VARCHAR,Price FLOAT,FinanceRate FLOAT,TradeReference VARCHAR,ParentTradeReference VARCHAR,TRSTradeRef VARCHAR,TGID VARCHAR,SEMEID VARCHAR,TradeStatus VARCHAR,SettleStatus VARCHAR,SettlementTypeDescription VARCHAR,SettlementCurrency VARCHAR,BankStatus VARCHAR,BankStatusDesciption VARCHAR,BankStatusCategory VARCHAR,Origin VARCHAR,LinkType VARCHAR,OverdueDays INTEGER,RrNo VARCHAR,Exposure FLOAT,CompanyGlossReference VARCHAR,ClearanceCode VARCHAR,OpenQuantity FLOAT,OpenConsideration FLOAT,BsNarrative VARCHAR,IntNarrative VARCHAR,UserName VARCHAR,CpartyExternalReference VARCHAR,ExchangeRate FLOAT,ExchangeRateIndicator VARCHAR,SettlementDiv VARCHAR,SettleCode VARCHAR,Postage FLOAT,Stamp FLOAT,Tariff FLOAT,Vat FLOAT,Levy FLOAT,GrossCredit FLOAT,Commission FLOAT,SpecialInstr1 VARCHAR,SpecialInstr2 VARCHAR,SpecialInstr3 VARCHAR,SpecialInstr4 VARCHAR,TradeVersion INTEGER,TimeStamp VARCHAR,tradeNo INTEGER,instNumber INTEGER,service VARCHAR,ProcStatus VARCHAR, BROKER_SHRT_NME VARCHAR, BROKER_COVERAGE_NAME VARCHAR, OpenConsiderationUSD FLOAT, RAG VARCHAR, AGE VARCHAR, MARKET VARCHAR, BUYIN VARCHAR, REGION VARCHAR, RPT_MKT_REGION VARCHAR, ASSIGN_TO VARCHAR, SIDE VARCHAR, RELATIONSHIP_ID VARCHAR, BUSINESS VARCHAR, RelationshipOrEntityId VARCHAR, ENTITY_ID VARCHAR, FAMILY_SNAME VARCHAR, TRADE_TYPE_DESC VARCHAR,ReportTradeType VARCHAR,TradeCategoryDesc VARCHAR,TradeGroupType VARCHAR, AGE_BUCKET VARCHAR, PENDING_UPDATE BOOLEAN, AUTOFX_IND BOOLEAN, KEY_CLIENT_IND BOOLEAN, HIGH_VALUE_QTY_IND BOOLEAN, MISSING_SSI_IND BOOLEAN, ManualStatusInd BOOLEAN,StorageStatus VARCHAR)";
    }

    
    public static String[] createColumnNames() {
        String[] columnNames = {"record_no", "FirmAccount", "FirmAccountDescription", "AdpInstrumentRef", "AdpInstrumentRefDesc", "TradingCurrency", "IsinReference", "PrimaryQuantity", "SettleQuantity", "CounterpartyReference", "CounterpartyDescription", "TradeType", "OperationType", "OperationTypeDescription", "NetConsideration", "PrincipalTradeCurrency", "PrincipalSettlingCurrency", "AccruedInterest", "TradeDate", "ValueDate", "ActualSettleDate", "Price", "FinanceRate", "TradeReference", "ParentTradeReference", "TRSTradeRef", "TGID", "SEMEID", "TradeStatus", "SettleStatus", "SettlementTypeDescription", "SettlementCurrency", "BankStatus", "BankStatusDesciption", "BankStatusCategory", "Origin", "LinkType", "OverdueDays", "RrNo", "Exposure", "CompanyGlossReference", "ClearanceCode", "OpenQuantity", "OpenConsideration", "BsNarrative", "IntNarrative", "UserName", "CpartyExternalReference", "ExchangeRate", "ExchangeRateIndicator", "SettlementDiv", "SettleCode", "Postage", "Stamp", "Tariff", "Vat", "Levy", "GrossCredit", "Commission", "SpecialInstr1", "SpecialInstr2", "SpecialInstr3", "SpecialInstr4", "TradeVersion", "TimeStamp", "tradeNo", "instNumber", "service", "ProcStatus", "BROKER_SHRT_NME", "BROKER_COVERAGE_NAME", "OpenConsiderationUSD", "RAG", "AGE", "MARKET", "BUYIN", "REGION", "RPT_MKT_REGION", "ASSIGN_TO", "SIDE", "RELATIONSHIP_ID", "BUSINESS", "RelationshipOrEntityId", "ENTITY_ID", "FAMILY_SNAME", "TRADE_TYPE_DESC", "ReportTradeType", "TradeCategoryDesc", "TradeGroupType", "AGE_BUCKET", "PENDING_UPDATE", "AUTOFX_IND", "KEY_CLIENT_IND", "HIGH_VALUE_QTY_IND", "MISSING_SSI_IND", "ManualStatusInd", "StorageStatus"};
        return columnNames;
    }
    
    public static Object[] createColumnValues() {
        Object[] columnValues;
        try {
        columnValues = new Object[] {1, "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", 123456, 123456, "Valor", "Valor", "Valor", "Valor", "Valor", 123456, 123456, 123456, 123456, "Valor", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2015-03-19 16:02:30"), "Valor", 123456, 123456, "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", 123456, "Valor", 123456, "Valor", "Valor", 123456, 123456, "Valor", "Valor", "Valor", "Valor", 123456, "Valor", "Valor", "Valor", 123456, 123456, 123456, 123456, 123456, 123456, 123456, "Valor", "Valor", "Valor", "Valor", 123456, "Valor", 123456, 123456, "Valor", "Valor", "Valor", "Valor", 123456, "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", "Valor", true, true, true, true, true, true, "Valor"};
        } catch (ParseException e) {
            return new Object[] {}; 
        }
        return columnValues;
    }

    public static String selectAllTradeTable() {
        return "SELECT * FROM TRADE";
    }
}
