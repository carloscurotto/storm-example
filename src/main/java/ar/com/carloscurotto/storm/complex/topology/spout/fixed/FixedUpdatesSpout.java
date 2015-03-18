package ar.com.carloscurotto.storm.complex.topology.spout.fixed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import ar.com.carloscurotto.storm.complex.Update;
import ar.com.carloscurotto.storm.complex.UpdateRow;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FixedUpdatesSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private Update[] updates;
    private int index;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        collector = theCollector;
        Update first = createUpdateFor("SEMS", "row-1");
        Update second = createUpdateFor("ANOTHER", "row-2");
        updates = new Update[] { first, second };
    }

    private Update createUpdateFor(final String theSystemId, final String theRowId) {
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("parameter-key1", "parameter-value1");
        
        Collection<UpdateRow> rows = new ArrayList<UpdateRow>();
        Map<String, Object> keyColumns = new HashMap<String, Object>();
        
        keyColumns.put("key-column1", "key-value1");
        Map<String, Object> updateColumns = new HashMap<String, Object>();
        
        updateColumns.put("update-column1", "update-value1");
        
        UpdateRow row = new UpdateRow(theRowId, keyColumns, updateColumns);
        rows.add(row);
        
        return new Update(theSystemId, parameters, rows);
    }

    @Override
    public void close() {
        updates = null;
    }

    @Override
    public void nextTuple() {
        if (index < updates.length) {
            Update update = updates[index];
            collector.emit(new Values(update));
            index++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declare(new Fields("update"));
    }
}
