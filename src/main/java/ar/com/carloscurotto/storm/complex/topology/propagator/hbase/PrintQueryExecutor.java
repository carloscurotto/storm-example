package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintQueryExecutor implements QueryExecutor {
    
    private static final long serialVersionUID = 1L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintQueryExecutor.class);

    @Override
    public void execute(final String theQuery) {
        LOGGER.info("Executing query [" + theQuery + "]");
    }
    
}
