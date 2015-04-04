package ar.com.carloscurotto.storm.complex.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;

public class UpdateServiceRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateServiceRunner.class);

    public static void main(String[] args) {

//        UpdatesTopologyConfiguration configuration = new UpdatesTopologyConfiguration();
//
//        UpdateService service = configuration.getUpdateService();
//        service.open();
//
//        Update firstUpdate = createUpdateFor("id-1", "SEMS", "row-1");
//        Result firstResult = service.submit(firstUpdate);
//
//        LOGGER.info("First result: " + firstResult);
//
//        Update secondUpdate = createUpdateFor("id-2", "ANOTHER", "row-2");
//        Result secondResult = service.submit(secondUpdate);
//
//        LOGGER.info("Second result: " + secondResult);
//
//        service.close();

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

}
