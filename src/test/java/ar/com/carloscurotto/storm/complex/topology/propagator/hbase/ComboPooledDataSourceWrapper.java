package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class ComboPooledDataSourceWrapper {//extends AbstractPoolBackedDataSource implements PooledDataSource, Serializable, Referenceable{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NoCommitConnection.class);
    
    private ComboPooledDataSource comboPooledDataSource;
    
    public ComboPooledDataSourceWrapper(final ComboPooledDataSource theComboPooledDataSource) {
        comboPooledDataSource = theComboPooledDataSource;
    }
    
    public Connection getConnection() throws SQLException {
        return new NoCommitConnection(comboPooledDataSource.getConnection());
    }

    
}
