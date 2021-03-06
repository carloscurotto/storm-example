package ar.com.carloscurotto.storm.updates.kafka;

import java.util.Map;

import ar.com.carloscurotto.storm.updates.fixed.repository.GlossUpdateCountsRepository;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GlossUpdatesBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private GlossUpdateCountsRepository counts;

    public GlossUpdatesBolt(GlossUpdateCountsRepository theCounts) {
        this.counts = theCounts;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TopologyContext theTopologyContext, OutputCollector theCollector) {
        collector = theCollector;
    }

    @Override
    public void execute(Tuple theTuple) {
        String update = theTuple.getString(0);
        Integer count = counts.get(update);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        counts.put(update, count);
        collector.emit("hbase-stream", theTuple, new Values(update));
        collector.ack(theTuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declareStream("hbase-stream", new Fields("update"));
    }

}
