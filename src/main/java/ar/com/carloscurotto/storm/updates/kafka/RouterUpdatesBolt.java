package ar.com.carloscurotto.storm.updates.kafka;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RouterUpdatesBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TopologyContext theTopologyContext, OutputCollector theCollector) {
        collector = theCollector;
    }

    @Override
    public void execute(Tuple theTuple) {
        String update = theTuple.getString(0);
        if (update.contains("gloss")) {
            collector.emit("gloss-stream", new Values(update));
        } else {
            collector.emit("hbase-stream", new Values(update));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declareStream("gloss-stream", new Fields("update"));
        theDeclarer.declareStream("hbase-stream", new Fields("update"));
    }

}
