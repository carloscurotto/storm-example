package ar.com.carloscurotto.storm.wordcount.serialized;

import java.util.Map;

import ar.com.carloscurotto.storm.wordcount.serialized.domain.Sentence;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FixedSentencesSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private Sentence[] sentences;
    private int index;

    public FixedSentencesSpout(Sentence[] theSentences) {
        sentences = theSentences;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map theConfiguration, TopologyContext theTopologyContext, SpoutOutputCollector theCollector) {
        collector = theCollector;
    }

    @Override
    public void nextTuple() {
        if (index < sentences.length) {
            Sentence sentence = sentences[index];
            collector.emit(new Values(sentence));
            index++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
