package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.CommentGlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

public class CommentGlossMessageBuilder extends GlossMessageBuilder {

    @Override
    public GlossMessage doBuild(UpdateRow theUpdateRow) {
        Validate.notNull(theUpdateRow, "The update row cannot be null.");
        String theInternalComments = (String) theUpdateRow.getUpdateColumnValue("internalComments");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");
        String theTradeNumber = (String) theUpdateRow.getUpdateColumnValue("tradeNo");

        return new CommentGlossMessage(theInternalComments, theUserId, theTradeNumber);
    }

    @Override
    protected boolean shouldBuild(final Map<String, Object> parameters) {
        return isParameterValueTrue(parameters, "updateInternalComment");
    }
}
