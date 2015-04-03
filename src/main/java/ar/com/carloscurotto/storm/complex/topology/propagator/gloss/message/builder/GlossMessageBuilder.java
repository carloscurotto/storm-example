package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

/**
 * Builds the different kind of {@link GlossMessage}s that will be propagated into Gloss subsystems.
 *
 * @author D540601
 */
public interface GlossMessageBuilder {
    /**
     * Builds a message that will be propagated using the information contained in the given {@link UpdateRow}
     *
     * @param theUpdateRow
     *            the update row with the information for the update.
     * @return a {@link GlossMessage} with the update information that will be propagated.
     */
    GlossMessage build(final UpdateRow theUpdateRow);

}