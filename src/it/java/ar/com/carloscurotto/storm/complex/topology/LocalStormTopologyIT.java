package ar.com.carloscurotto.storm.complex.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.UpdateService;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/app-config.xml" })
public class LocalStormTopologyIT {

    private static final String BROKER_URL = "tcp://localhost:61616";

    private BrokerService brokerService;
    private LocalCluster localCluster;

    @Autowired
    private UpdateService updateService;

    @Autowired
    private UpdateTopologyConfiguration updateTopologyConfiguration;

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.addConnector(BROKER_URL);
        brokerService.start();
    }

    @Test
    public void test() {

        Config config = new Config();
        config.setDebug(false);
        localCluster = new LocalCluster();
        localCluster.submitTopology("complex-updates", config, updateTopologyConfiguration.getStormTopology());
        updateService.open();

        Utils.sleep(5000);
        Update firstUpdate = createUpdateFor("id-1", "SEMS", "row-1");
        Result firstResult = updateService.submit(firstUpdate);

        System.out.println("First result: " + firstResult);
        Update secondUpdate = createUpdateFor("id-2", "ANOTHER", "row-2");
        Result secondResult = updateService.submit(secondUpdate);
        System.out.println("Second result: " + secondResult);

    }

    // TODO move this method somewhere else, it is repeated with FixedUpdatesSpout
    private static Update createUpdateFor(final String theId, final String theSystemId, final String theRowId) {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("parameter-key1", "parameter-value1");

        Collection<UpdateRow> rows = new ArrayList<UpdateRow>();
        Map<String, Object> keyColumns = new HashMap<String, Object>();

        keyColumns.put("key-column1", "key-value1");
        Map<String, Object> updateColumns = new HashMap<String, Object>();

        updateColumns.put("update-column1", "update-value1");

        UpdateRow row = new UpdateRow(theRowId, keyColumns, updateColumns);
        rows.add(row);

        return new Update(theId, theSystemId, "table", parameters, rows);
    }

    @After
    public void after() throws Exception {
        localCluster.killTopology("complex-updates");
        localCluster.shutdown();
        brokerService.stop();
        updateService.close();
    }
}
