package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.AbstractUpdatePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * @author N619614
 *
 */
public class HBaseUpdatePropagator extends AbstractUpdatePropagator {

    private static final long serialVersionUID = 1L;

    /**
     * The builder that creates the query.
     */
    public QueryBuilder queryBuilder;

    /**
     * The data source to execute the query.
     */
    private DataSource dataSource;

    /**
     * Creates an {@link HBaseUpdatePropagator} for the given query builder and data source.
     *
     * @param theQueryBuilder
     *            the given query builder.
     * @param theDataSource
     *            the given data source.
     */
    public HBaseUpdatePropagator(final QueryBuilder theQueryBuilder, final DataSource theDataSource) {
        Validate.notNull(theQueryBuilder, "The query builder can not be null.");
        Validate.notNull(theDataSource, "The data source can not be null.");
        queryBuilder = theQueryBuilder;
        dataSource = theDataSource;
    }

    @Override
    protected UpdatePropagatorResult doExecute(UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The Context cannot be null.");
        try {
            String upsertQuery = createUpsertQuery(theContext);
            executeUpsertQuery(upsertQuery);
            return UpdatePropagatorResult.createSuccess("Row succefully updated.");
        } catch (Exception e) {
            return UpdatePropagatorResult.createFailure(e.getMessage());
        }
    }

    private String createUpsertQuery(final UpdatePropagatorContext theUpdate) {
        return queryBuilder.build(theUpdate);
    }

    private void executeUpsertQuery(final String upsertQuery) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(upsertQuery);
            int updateCount = statement.getUpdateCount();
            if (updateCount != 1) {
                connection.rollback();
                throw new RuntimeException("Rolling back query [" + upsertQuery + "], it matched more than one result.");
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Something went wrong while executing the query");
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }

}
