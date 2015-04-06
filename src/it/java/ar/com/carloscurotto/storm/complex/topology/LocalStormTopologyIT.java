package ar.com.carloscurotto.storm.complex.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.ResultRow;
import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.service.UpdateService;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/app-config.xml" })
public class LocalStormTopologyIT {

    private static final String BROKER_URL = "tcp://localhost:61616";

    private BrokerService brokerService;

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

    /**
     * Encontre que Nathan hace referencia a testear la topology basandose en los siguientes ejemplos:
     * https://github.com/xumingming/storm-lib/blob/master/src/jvm/storm/TestingApiDemo.java
     *
     */
    @Test
    public void test() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(2);
        mkClusterParam.setPortsPerSupervisor(5);
        final Config daemonConf = new Config();

        Testing.withLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(final ILocalCluster cluster) throws AlreadyAliveException, InvalidTopologyException {
                cluster.submitTopology("complex-updates", daemonConf, updateTopologyConfiguration.getStormTopology());
                updateService.open();
                Update firstUpdate = createUpdateFor("id-1", "SEMS", "row-1");
                Result firstResult = updateService.submit(firstUpdate);
                assertSuccessfullResult(firstUpdate, firstResult);
                Update secondUpdate = createUpdateFor("id-2", "ANOTHER", "row-2");
                Result secondResult = updateService.submit(secondUpdate);
                assertSuccessfullResult(secondUpdate, secondResult);
            }
        });
    }

    private static void assertSuccessfullResult(final Update theUpdate, final Result theResult) {
        assertThat("The update id does not match the result id.", theUpdate.getId(), equalTo(theResult.getId()));
        assertThat("The update rows quantity not match the result rows quantity.", theUpdate.getRows().size(), equalTo(theResult.getRows().size()));
        Collection<UpdateRow> updateRows = theUpdate.getRows();
        for (UpdateRow updateRow : updateRows) {
            ResultRow resultRow = theResult.getRow(updateRow.getId());
            assertThat("An update row id does not match its result row id.", updateRow.getId(), equalTo(resultRow.getId()));
            assertThat("An update row was not successfull.", resultRow.isSuccessful(), equalTo(Boolean.TRUE));
        }
    }

    // TODO move this method somewhere else, it is repeated with FixedUpdatesSpout
    private static Update createUpdateFor(final String theId, final String theSystemId, final String theRowId) {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("updateInternalComment", true);
        parameters.put("update", true);
        parameters.put("exceptionTrade", true);

        Collection<UpdateRow> rows = new ArrayList<UpdateRow>();

        Map<String, Object> keyColumns = new HashMap<String, Object>();
        keyColumns.put("key-column1", "key-value1");

        Map<String, Object> updateColumns = new HashMap<String, Object>();
        updateColumns.put("internalComments", "test internal comments");
        updateColumns.put("userId", "O605461");
        updateColumns.put("tradeNo", "12554654");
        updateColumns.put("externalComments", "test external comments");
        updateColumns.put("statusCode", "200");
        updateColumns.put("instNumber", "123554");
        updateColumns.put("service", "test service");

        UpdateRow row = new UpdateRow(theRowId, System.currentTimeMillis(), keyColumns, updateColumns);
        rows.add(row);

        return new Update(theId, theSystemId, "table", parameters, rows);
    }

    @After
    public void after() throws Exception {
        brokerService.stop();
        updateService.close();
    }
}
