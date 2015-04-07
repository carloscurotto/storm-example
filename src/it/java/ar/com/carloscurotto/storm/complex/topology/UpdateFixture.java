package ar.com.carloscurotto.storm.complex.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.model.UpdateRow;

public final class UpdateFixture {

    private UpdateFixture() {
    }

    /**
     * Creates a dummy update with the given system id and row id with only one row.
     *
     * @param theSystemId
     *            the system id.
     * @param theRowId
     *            the id of the unique row inside the udpate.
     * @return the dummy update created.
     */
    public static final Update createUpdateFor(final String theSystemId, final String theRowId) {
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

        UpdateRow row = new UpdateRow(theRowId, System.currentTimeMillis(), keyColumns,
                updateColumns);
        rows.add(row);

        return new Update(theSystemId, "table", parameters, rows);
    }

}
