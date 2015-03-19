package com.connection;

import java.beans.PropertyVetoException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import com.mchange.v2.c3p0.ComboPooledDataSource;

@Configuration
@PropertySource({ "c3p0.properties", "database.properties" })
public class MainConfig {

    @Autowired
    private Environment environment;

    @Bean(destroyMethod = "close")
    public DataSource getDataSource() {
        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        try {
            comboPooledDataSource.setDriverClass(environment.getProperty("db.driver"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException("Couldn't load driver class", e);
        }
        comboPooledDataSource.setJdbcUrl(environment.getProperty("db.url"));
        comboPooledDataSource.setInitialPoolSize(environment.getProperty("c3p0.initialPoolSize",
                Integer.class));
        comboPooledDataSource.setMinPoolSize(environment.getProperty("c3p0.minPoolSize",
                Integer.class));
        comboPooledDataSource.setInitialPoolSize(environment.getProperty("c3p0.maxPoolSize",
                Integer.class));
        comboPooledDataSource.setInitialPoolSize(environment.getProperty("c3p0.acquireIncrement",
                Integer.class));
        comboPooledDataSource.setInitialPoolSize(environment.getProperty("c3p0.maxIdleTime",
                Integer.class));
        return comboPooledDataSource;
    }

    
}
