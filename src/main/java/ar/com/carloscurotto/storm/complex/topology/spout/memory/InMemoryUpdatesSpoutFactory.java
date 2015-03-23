package ar.com.carloscurotto.storm.complex.topology.spout.memory;

import backtype.storm.topology.IRichSpout;
import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;

public class InMemoryUpdatesSpoutFactory implements UpdatesSpoutFactory {

    @Override
    public IRichSpout create() {
        return new InMemoryUpdatesSpout();
    }
}
