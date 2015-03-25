package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class QueryExecutorTest {

    private DataSourceQueryExecutor queryExecutor;

    @Mock
    private DataSource dataSourceMock;

    @Mock
    private Connection connectionMock;

    @Mock
    private Statement statementMock;

    @Before
    public void setUp() {
        queryExecutor = new DataSourceQueryExecutor(dataSourceMock);
    }

    @Test(expected = NullPointerException.class)
    public void newInstanceNullDataSource() {
        queryExecutor = new DataSourceQueryExecutor(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeEmptyQuery() {
        queryExecutor.execute("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeQueryWithMultipleSpaces() {
        queryExecutor.execute("    ");
    }

    @Test(expected = NullPointerException.class)
    public void executeNullQuery() {
        queryExecutor.execute(null);
    }

    @Test(expected = RuntimeException.class)
    public void executeGetConnectionThrowsException() throws SQLException {
        expect(dataSourceMock.getConnection()).andThrow(new SQLException());
        replay(dataSourceMock);
        queryExecutor.execute("query");
        verify(dataSourceMock);
    }

    @Test(expected = RuntimeException.class)
    public void executeCreateStatementThrowsException() throws SQLException {
        expect(dataSourceMock.getConnection()).andReturn(connectionMock);
        expect(connectionMock.createStatement()).andThrow(new SQLException());
        connectionMock.close();
        expectLastCall();
        replay(dataSourceMock, connectionMock);
        queryExecutor.execute("query");
        verify(dataSourceMock, connectionMock);
    }

    @Test(expected = RuntimeException.class)
    public void executeGetUpdateCountThrowsException() throws SQLException {
        expect(dataSourceMock.getConnection()).andReturn(connectionMock);
        expect(connectionMock.createStatement()).andReturn(statementMock);
        expect(statementMock.execute(eq("query"))).andReturn(true);
        expect(statementMock.getUpdateCount()).andThrow(new SQLException());
        connectionMock.close();
        expectLastCall();
        replay(dataSourceMock, connectionMock, statementMock);
        queryExecutor.execute("query");
        verify(dataSourceMock, connectionMock, statementMock);
    }

    @Test(expected = RuntimeException.class)
    public void executeCommitThrowsException() throws SQLException {
        expect(dataSourceMock.getConnection()).andReturn(connectionMock);
        expect(connectionMock.createStatement()).andReturn(statementMock);
        expect(statementMock.execute(eq("query"))).andReturn(true);
        expect(statementMock.getUpdateCount()).andReturn(1);
        connectionMock.commit();
        expectLastCall().andThrow(new SQLException());
        connectionMock.close();
        expectLastCall();
        replay(dataSourceMock, connectionMock, statementMock);
        queryExecutor.execute("query");
        verify(dataSourceMock, connectionMock, statementMock);
    }

    @Test(expected = RuntimeException.class)
    public void executeInvalidUpdateCount() throws SQLException {
        expect(dataSourceMock.getConnection()).andReturn(connectionMock);
        expect(connectionMock.createStatement()).andReturn(statementMock);
        expect(statementMock.execute(eq("query"))).andReturn(true);
        expect(statementMock.getUpdateCount()).andReturn(0);
        connectionMock.rollback();
        expectLastCall();
        connectionMock.close();
        expectLastCall();
        replay(dataSourceMock, connectionMock, statementMock);
        queryExecutor.execute("query");
        verify(dataSourceMock, connectionMock, statementMock);
    }

    @Test
    public void execute() throws SQLException {
        expect(dataSourceMock.getConnection()).andReturn(connectionMock);
        expect(connectionMock.createStatement()).andReturn(statementMock);
        expect(statementMock.execute(eq("query"))).andReturn(true);
        expect(statementMock.getUpdateCount()).andReturn(1);
        connectionMock.commit();
        expectLastCall();
        connectionMock.close();
        expectLastCall();
        replay(dataSourceMock, connectionMock, statementMock);
        queryExecutor.execute("query");
        verify(dataSourceMock, connectionMock, statementMock);
    }
}
