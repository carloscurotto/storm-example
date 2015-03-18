package ar.com.carloscurotto.storm.complex.topology.spout.fixed;

import ar.com.carloscurotto.storm.complex.topology.spout.UpdatesSpoutFactory;
import backtype.storm.topology.IRichSpout;

/**
 * Creates a spout that will produce a fixed quantity of updates.
 *
 * @author O605461
 *
 */
public class FixedUpdatesSpoutFactory implements UpdatesSpoutFactory {

    @Override
    public IRichSpout create() {
	return new FixedUpdatesSpout();
    }

}
