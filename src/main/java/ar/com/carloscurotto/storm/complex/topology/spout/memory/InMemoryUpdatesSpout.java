package ar.com.carloscurotto.storm.complex.topology.spout.memory;

import java.util.Map;

import ar.com.carloscurotto.storm.complex.model.Update;
import ar.com.carloscurotto.storm.complex.transport.memory.queue.InMemoryUpdatesQueue;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class InMemoryUpdatesSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private static final int TUPLE_SLEEP_MILLIS = 1000 * 5;

    private InMemoryUpdatesQueue updates = new InMemoryUpdatesQueue();
    private SpoutOutputCollector collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        collector = theCollector;
        updates.open();
    }
    
    @Override
    public void close() {
        updates.close();
        collector = null;
    }

    @Override
    public void nextTuple() {
        Update update = updates.poll();
        if (update != null) {
            collector.emit(new Values(update));
        }
        sleep(TUPLE_SLEEP_MILLIS);
    }
    
    private void sleep(long theMillis) {
        try {
            Thread.sleep(theMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error sleeping", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer theDeclarer) {
        theDeclarer.declare(new Fields("update"));
    }
    
}