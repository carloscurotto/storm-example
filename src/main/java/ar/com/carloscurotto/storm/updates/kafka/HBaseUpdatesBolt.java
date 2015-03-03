package ar.com.carloscurotto.storm.updates.kafka;

import java.util.Map;

import ar.com.carloscurotto.storm.updates.fixed.repository.HBaseUpdateCountsRepository;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HBaseUpdatesBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private HBaseUpdateCountsRepository counts;

    public HBaseUpdatesBolt(HBaseUpdateCountsRepository theCounts) {
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
        System.out.println("Update tuple received [" + update + ", " + count + "]");
        collector.ack(theTuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
