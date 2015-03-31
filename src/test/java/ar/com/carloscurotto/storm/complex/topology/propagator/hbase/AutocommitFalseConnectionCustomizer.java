package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ConnectionCustomizer;

public class AutocommitFalseConnectionCustomizer implements ConnectionCustomizer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AutocommitFalseConnectionCustomizer.class); 

    @Override
    public void onAcquire(final Connection theConnection, final String theParentDataSourceIdentityToken) throws Exception {
        LOGGER.debug("onAcquire called");
    }

    @Override
    public void onDestroy(final Connection theConnection, final String theParentDataSourceIdentityToken) throws Exception {
        LOGGER.debug("onDestroy called");
    }

    @Override
    public void onCheckOut(final Connection theConnection, final String theParentDataSourceIdentityToken) throws Exception {
        LOGGER.debug("onCheckOut called");
        theConnection.setAutoCommit(false);
    }

    @Override
    public void onCheckIn(final Connection theConnection, final String theParentDataSourceIdentityToken) throws Exception {
        LOGGER.debug("onCheckIn called");
    }

}
