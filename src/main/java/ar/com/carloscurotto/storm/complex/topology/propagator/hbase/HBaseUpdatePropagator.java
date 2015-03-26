package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.service.OpenAwarePropagator;
import ar.com.carloscurotto.storm.complex.topology.propagator.context.UpdatePropagatorContext;
import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

/**
 * This propagator is responsible for translating the incoming update into a query that is run against hbase. It marks
 * the update for a single row to be in a transient state.
 *
 * @author N619614
 */
public class HBaseUpdatePropagator extends OpenAwarePropagator<UpdatePropagatorContext, UpdatePropagatorResult> {

    private static final long serialVersionUID = 1L;

    /**
     * The builder that creates the query.
     */
    private QueryBuilder queryBuilder;

    /**
     * The executor for the query returned by the query builder.
     */
    private QueryExecutor queryExecutor;

    /**
     * Creates an {@link HBaseUpdatePropagator} for the given query builder and query executor.
     *
     * @param theQueryBuilder
     *            the given query builder. It cannot be null.
     * @param theQueryExecutor
     *            the given query executor. It cannot be null.
     */
    public HBaseUpdatePropagator(final QueryBuilder theQueryBuilder, final QueryExecutor theQueryExecutor) {
        Validate.notNull(theQueryBuilder, "The query builder can not be null.");
        Validate.notNull(theQueryExecutor, "The query executor can not be null.");
        queryBuilder = theQueryBuilder;
        queryExecutor = theQueryExecutor;
    }

    @Override
    protected UpdatePropagatorResult doPropagate(UpdatePropagatorContext theContext) {
        Validate.notNull(theContext, "The Context cannot be null.");
        try {
            queryExecutor.execute(queryBuilder.build(theContext));
            return UpdatePropagatorResult.createSuccess("Row succefully updated.");
        } catch (Exception e) {
            return UpdatePropagatorResult.createFailure(e.getMessage());
        }
    }

    @Override
    protected void doOpen() {
    }

    @Override
    protected void doClose() {
    }
}
