package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.ResultStatus;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * @author N619614
 *
 */
@RunWith(EasyMockRunner.class)
public class HBaseUpdatePropagatorTest {

    private HBaseUpdatePropagator hbaseInternalUpdatePropagator;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private UpdatePropagatorContext updatePropagatorContextMock;

    @Mock
    private QueryBuilder queryBuilderMock;

    @Mock
    private DataSourceQueryExecutor queryExecutorMock;

    @Mock
    private UpdatePropagatorContext context;

    @Mock
    private DataSource dataSourceMock;

    @Mock
    private Connection connectionMock;

    @Mock
    private Statement statementMock;

    @Before
    public void setUp() {
        hbaseInternalUpdatePropagator = new HBaseUpdatePropagator(queryBuilderMock,
                queryExecutorMock);
    }

    @Test(expected = NullPointerException.class)
    public void newInstanceNullQueryBuilder() {
        new HBaseUpdatePropagator(null, queryExecutorMock);
    }

    @Test(expected = NullPointerException.class)
    public void newInstanceNullQueryExecutor() {
        new HBaseUpdatePropagator(queryBuilderMock, null);
    }

    @Test
    public void executeWithNonNullUpdateAndUpdatePropagatorResultIsSuccess() throws SQLException {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
        expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
        queryExecutorMock.execute("upsertQuery");
        expectLastCall();
        replay(updatePropagatorContextMock, queryBuilderMock, queryExecutorMock);
        UpdatePropagatorResult updatePropagatorResult = hbaseInternalUpdatePropagator
                .execute(updatePropagatorContextMock);
        assertThat(updatePropagatorResult.getStatus(), equalTo(ResultStatus.SUCCESS));
        assertThat(updatePropagatorResult.getMessage(), equalTo("Row succefully updated."));
        verify(updatePropagatorContextMock, queryBuilderMock, queryExecutorMock);
    }

    @Test
    public void executeWithNonNullUpdateAndUpdatePropagatorResultIsNotSuccess() throws SQLException {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
        expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
        queryExecutorMock.execute("upsertQuery");
        expectLastCall().andThrow(new RuntimeException("An error occurred"));
        replay(updatePropagatorContextMock, queryBuilderMock, queryExecutorMock);
        UpdatePropagatorResult updatePropagatorResult = hbaseInternalUpdatePropagator
                .execute(updatePropagatorContextMock);
        assertThat(updatePropagatorResult.getStatus(), equalTo(ResultStatus.FAILURE));
        assertThat(updatePropagatorResult.getMessage(), equalTo("An error occurred"));
        verify(updatePropagatorContextMock, queryBuilderMock, queryExecutorMock);
    }

    @Test
    public void doExecuteNullUpdatePropagatorContext() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(equalTo("The Context cannot be null."));
        hbaseInternalUpdatePropagator.doExecute(null);
    }

    @Test
    public void doExecute() {
        expect(queryBuilderMock.build(context)).andReturn("Query");
        queryExecutorMock.execute(eq("Query"));
        expectLastCall();
        replay(queryBuilderMock, queryExecutorMock);
        UpdatePropagatorResult result = hbaseInternalUpdatePropagator.doExecute(context);
        verify(queryBuilderMock, queryExecutorMock);
        assertThat(result.getStatus(), equalTo(ResultStatus.SUCCESS));
        assertThat(result.getMessage(), equalTo("Row succefully updated."));
    }

    @Test
    public void doExecuteExecutorThrowsException() {
        expect(queryBuilderMock.build(context)).andReturn("Query");
        queryExecutorMock.execute(eq("Query"));
        expectLastCall().andThrow(new RuntimeException("An error occurred"));
        replay(queryBuilderMock, queryExecutorMock);
        UpdatePropagatorResult result = hbaseInternalUpdatePropagator.doExecute(context);
        verify(queryBuilderMock, queryExecutorMock);
        assertThat(result.getStatus(), equalTo(ResultStatus.FAILURE));
        assertThat(result.getMessage(), equalTo("An error occurred"));

    }

    @Test
    public void open() throws SQLException {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
        hbaseInternalUpdatePropagator.open();
        boolean isOpen = (Boolean) ReflectionTestUtils.getField(hbaseInternalUpdatePropagator,
                "isOpen");
        assertTrue(isOpen);
    }

    @Test
    public void closeOpened() {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
        hbaseInternalUpdatePropagator.close();
        boolean isOpen = hbaseInternalUpdatePropagator.isOpen();
        assertFalse(isOpen);
    }

    @Test
    public void closeClosed() {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
        hbaseInternalUpdatePropagator.close();
        boolean isOpen = hbaseInternalUpdatePropagator.isOpen();
        assertFalse(isOpen);
    }

    @Test(expected = IllegalStateException.class)
    public void openOpened() {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
        hbaseInternalUpdatePropagator.open();
    }

    @Test
    public void openClosed() {
        ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
        hbaseInternalUpdatePropagator.open();
        assertTrue(hbaseInternalUpdatePropagator.isOpen());
    }
}
