package ar.com.carloscurotto.storm.updates;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class FixedUpdatesSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private String[] updates;
    private int index;

    public FixedUpdatesSpout(String[] theUpdates) {
        updates = theUpdates;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        collector = theCollector;
    }

    @Override
    public void nextTuple() {
        if (index < updates.length) {
            String update = updates[index];
            if (update.contains("gloss")) {
                collector.emit("gloss-stream", new Values(update));
            } else {
                collector.emit("hbase-stream", new Values(update));
            }
            index++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declareStream("gloss-stream", new Fields("update"));
        theDeclarer.declareStream("hbase-stream", new Fields("update"));
    }

}
