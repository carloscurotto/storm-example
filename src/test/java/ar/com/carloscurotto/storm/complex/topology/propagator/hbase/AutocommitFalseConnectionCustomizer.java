package ar.com.carloscurotto.storm.complex.topology.propagator.hbase;

import java.sql.Connection;

import com.mchange.v2.c3p0.ConnectionCustomizer;

public class AutocommitFalseConnectionCustomizer implements ConnectionCustomizer {

    @Override
    public void onAcquire(Connection c, String parentDataSourceIdentityToken) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onDestroy(Connection c, String parentDataSourceIdentityToken) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onCheckOut(Connection c, String parentDataSourceIdentityToken) throws Exception {
        // TODO Auto-generated method stub
        c.setAutoCommit(false);
        
    }

    @Override
    public void onCheckIn(Connection c, String parentDataSourceIdentityToken) throws Exception {
        // TODO Auto-generated method stub
        
    }

}
