package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.Validate;

/**
 * This query executor runs a query against a database that is encapsulated by a given {@link DataSource}.
 *
 * @author N619614
 *
 */
public class DataSourceQueryExecutor implements QueryExecutor {

    /**
     * The data source from where this executor gets a connection.
     */
    private DataSource dataSource;

    /**
     * Constructs a {@link DataSourceQueryExecutor} with the given data source.
     *
     * @param theDataSource
     *            the data source from where this executor gets a connection. It cannot be null.
     */
    public DataSourceQueryExecutor(final DataSource theDataSource) {
        Validate.notNull(theDataSource, "The data source cannot be null");
        dataSource = theDataSource;
    }

    @Override
    public void execute(final String theQuery) {
        Validate.notBlank(theQuery, "The query to be executed cannot be blank");
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(theQuery);
            int updateCount = statement.getUpdateCount();
            if (updateCount != 1) {
                connection.rollback();
                throw new RuntimeException("Rolling back query [" + theQuery + "], it matched more than one result.");
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Something went wrong while executing the query: " + theQuery);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

}
