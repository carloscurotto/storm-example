package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixTestDriver;

public class PhoenixDriverNonCommit extends PhoenixTestDriver {

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.contains("jdbc:losiiii");
    }

    @Override
    public synchronized Connection connect(String url, Properties info) throws SQLException {
        Connection connection = super.connect(url, info);
        return new NoCommitConnection(connection);
    }
}
