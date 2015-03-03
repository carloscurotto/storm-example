package ar.com.carloscurotto.storm.wordcount.fixed;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    
    private OutputCollector collector;
    private WordCountsRepository counts;
    
    public WordCountBolt(WordCountsRepository theCounts) {
        counts = theCounts;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map theConfiguration, TopologyContext theTopologyContext, OutputCollector theCollector) {
        collector = theCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
