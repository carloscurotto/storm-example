package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

/**
 * Implementations are responsible for running a query against its corresponding destination.
 *
 * @author O605461, W506376
 *
 */
public interface QueryExecutor {

    /**
     * Executes the given query. Implementations should handle the exceptions that may arise.
     *
     * @param theQuery
     *            the query to be run.
     */
    void execute(String theQuery);

}
