package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PMetaData;

public class PhoenixDriverNonCommit extends PhoenixTestDriver {

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.contains("jdbc:losiiii");
    }
    
    @Override
    public synchronized Connection connect(String url, Properties info) throws SQLException {
        //Connection connection = super.connect("jdbc:phoenix:none;test=true", info);
        Connection connection = super.connect(url, info);
        return new NoCommitConnection(connection);
    }

/*    
    @Override
    public synchronized ConnectionQueryServices getConnectionQueryServices(String url,
            Properties info) throws SQLException {
        Method method;
        try {
            method = ConnectionInfo.class.getMethod("create", String.class);
            ConnectionInfo o = (ConnectionInfo) method.invoke(null, url);
            return new ConnectionQueryServicesNonCommit(this.getQueryServices(), o);
        } catch (Exception e) {
        }
        return null;
    }

    private class ConnectionQueryServicesNonCommit extends ConnectionlessQueryServicesImpl {

        public ConnectionQueryServicesNonCommit(QueryServices queryServices, ConnectionInfo connInfo) {
            super(queryServices, connInfo);
        }

        @Override
        public PhoenixConnection connect(String url, Properties info) throws SQLException {
            Object value = null;
            try {
                Field field = this.getClass().getDeclaredField("metaData");
                field.setAccessible(true);
                value = field.get(this);
            } catch (Exception e) {
            }
            return new CommitlessConnection(this, url, info, (PMetaData) value);
        }
    }

    private class CommitlessConnection extends PhoenixConnection {

        public CommitlessConnection(ConnectionQueryServices services, String url, Properties info,
                PMetaData metaData) throws SQLException {
            super(services, url, info, metaData);
        }

        @Override
        public void commit() throws SQLException {
            System.out.println("Skipping commit");
        }
    }
*/    
}
