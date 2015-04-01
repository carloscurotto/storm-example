package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.beans.PropertyVetoException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import com.mchange.v2.c3p0.ComboPooledDataSource;

@Configuration
@PropertySource({ "c3p0.properties", "database.properties" })
public class MainConfigForTest {
    
    private int initialPoolSize = 5;
    private int minPoolSize = 5;
    private int maxPoolSize = 15;
    private int acquireIncrement = 1;
    private int maxIdleTime = 300;

    @Autowired
    private Environment environment;

    @Bean(destroyMethod = "close")
    public DataSource getDataSource(String url) {
        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        try {
            DriverManager.registerDriver((Driver) Class.forName("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.PhoenixDriverNonCommit").newInstance());
            comboPooledDataSource.setDriverClass("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.PhoenixDriverNonCommit");
        } catch (PropertyVetoException | InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Couldn't load driver class", e);
        }
        comboPooledDataSource.setJdbcUrl(url);
        comboPooledDataSource.setInitialPoolSize(initialPoolSize);
        comboPooledDataSource.setMinPoolSize(minPoolSize);
        comboPooledDataSource.setInitialPoolSize(maxPoolSize);
        comboPooledDataSource.setInitialPoolSize(acquireIncrement);
        comboPooledDataSource.setInitialPoolSize(maxIdleTime);
        comboPooledDataSource.setConnectionCustomizerClassName("ar.com.carloscurotto.storm.complex.topology.propagator.hbase.AutocommitFalseConnectionCustomizer");
        return comboPooledDataSource;
    }

    
}
