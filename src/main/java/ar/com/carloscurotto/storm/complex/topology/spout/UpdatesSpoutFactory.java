package ar.com.carloscurotto.storm.complex.topology.spout;

import backtype.storm.topology.IRichSpout;

/**
 * Defines the factory of updates spouts.
 *
 * @author O605461
 *
 */
public interface UpdatesSpoutFactory {

    /**
     * Creates an updates spout.
     *
     * @return the updates spout created.
     */
    public IRichSpout create();

}
