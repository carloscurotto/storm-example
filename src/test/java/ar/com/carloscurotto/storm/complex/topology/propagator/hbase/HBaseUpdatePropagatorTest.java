/**
 *
 */
package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;


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
	private Connection connectionMock;

	@Mock
	private DataSource dataSourceMock;

	@Mock
	private Statement statementMock;

	@Before
	public void setUp() {
		hbaseInternalUpdatePropagator = new HBaseUpdatePropagator(queryBuilderMock, dataSourceMock);
	}

	@Test(expected = NullPointerException.class)
	public void newInstanceNullUpsertQuery() {
		new HBaseUpdatePropagator(null, dataSourceMock);
	}

	@Test(expected = NullPointerException.class)
	public void newInstanceNullDataSpurce() {
		new HBaseUpdatePropagator(queryBuilderMock, null);
	}

	@Test
	public void newInstanceWithUpsertQuery() {
		new HBaseUpdatePropagator(queryBuilderMock, dataSourceMock);
	}

	@Test
	public void executeWithNullUpdate() {
		hbaseInternalUpdatePropagator.doExecute(null);
	}

	@Test
	public void executeOpenWithNonNullUpdateAndNullRows() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		replay(updatePropagatorContextMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
		verify(updatePropagatorContextMock);
	}

	/**
	 * This test is no longer necessary because an update can not have empty rows.
	 * 
	 * @throws SQLException
	 */
	@Ignore
	@Test
	public void executeOpenWithNonNullUpdateAndEmptyRows() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		replay(updatePropagatorContextMock);
			ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
      assertNull(resultRow);
			verify(updatePropagatorContextMock);
	}

	@Test
	public void executeWithNonNullUpdateAndQueryResultIsExactlyOne() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
		expect(dataSourceMock.getConnection()).andReturn(connectionMock);
		expect(connectionMock.createStatement()).andReturn(statementMock);
		expect(statementMock.execute("upsertQuery")).andReturn(true);
		expect(statementMock.getUpdateCount()).andReturn(1);
		connectionMock.commit();
		DbUtils.closeQuietly(connectionMock);
		expectLastCall();
		replay(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
		verify(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
	}

	@Test
	public void executeWithNonNullUpdateAndQueryResultIsZero() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
		expect(dataSourceMock.getConnection()).andReturn(connectionMock);
		expect(connectionMock.createStatement()).andReturn(statementMock);
		expect(statementMock.execute("upsertQuery")).andReturn(true);
		expect(statementMock.getUpdateCount()).andReturn(0);
		connectionMock.rollback();
		expectLastCall();
		DbUtils.closeQuietly(connectionMock);
		expectLastCall();
		replay(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
			verify(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
	}

	@Test
	public void executeWithNonNullUpdateAndQueryResultIsMoreThanOne() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
		expect(dataSourceMock.getConnection()).andReturn(connectionMock);
		expect(connectionMock.createStatement()).andReturn(statementMock);
		expect(statementMock.execute("upsertQuery")).andReturn(true);
		expect(statementMock.getUpdateCount()).andReturn(2);
		connectionMock.rollback();
		DbUtils.closeQuietly(connectionMock);
		expectLastCall();
		replay(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
			verify(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
	}

	@Test
	public void executeOpenWithNonNullUpdateThrowsSqlExceptionAtConnection() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
		expect(dataSourceMock.getConnection()).andThrow(new SQLException());
		replay(updatePropagatorContextMock, queryBuilderMock, dataSourceMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
			verify(updatePropagatorContextMock, queryBuilderMock, dataSourceMock);
	}

	@Test
	public void executeOpenWithNonNullUpdateThrowsSqlExceptionAtExecute() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		expect(queryBuilderMock.build(updatePropagatorContextMock)).andReturn("upsertQuery");
		expect(dataSourceMock.getConnection()).andReturn(connectionMock);
		expect(connectionMock.createStatement()).andReturn(statementMock);
		expect(statementMock.execute("upsertQuery")).andThrow(new SQLException());
		DbUtils.closeQuietly(connectionMock);
		expectLastCall();
		replay(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
		ResultRow resultRow = hbaseInternalUpdatePropagator.execute(updatePropagatorContextMock);
    assertNull(resultRow);
			verify(statementMock, updatePropagatorContextMock, queryBuilderMock, connectionMock, dataSourceMock);
	}

	@Test
	public void open() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
		hbaseInternalUpdatePropagator.open();
		boolean isOpen = (Boolean) ReflectionTestUtils.getField(hbaseInternalUpdatePropagator, "isOpen");
		assertTrue(isOpen);
	}

	@Ignore
	@Test(expected = RuntimeException.class)
	public void openDataSourceThrowsRuntimeException() {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		hbaseInternalUpdatePropagator.open();
	}

	@Test
	public void closeOpenned() {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		hbaseInternalUpdatePropagator.close();
		boolean isOpen = (Boolean) ReflectionTestUtils.getField(hbaseInternalUpdatePropagator, "isOpen");
		assertFalse(isOpen);
	}

	@Test
	public void closeClosed() {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
		hbaseInternalUpdatePropagator.close();
		boolean isOpen = (Boolean) ReflectionTestUtils.getField(hbaseInternalUpdatePropagator, "isOpen");
		assertFalse(isOpen);
	}

	@Test
	public void isOpen() {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", true);
		assertTrue(hbaseInternalUpdatePropagator.isOpen());
	}

	@Test
	public void isOpenNot() throws SQLException {
		ReflectionTestUtils.setField(hbaseInternalUpdatePropagator, "isOpen", false);
		assertFalse(hbaseInternalUpdatePropagator.isOpen());
	}

}
