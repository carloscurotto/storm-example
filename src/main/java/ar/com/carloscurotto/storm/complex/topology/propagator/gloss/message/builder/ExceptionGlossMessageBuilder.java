package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionGlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

public class ExceptionGlossMessageBuilder implements GlossMessageBuilder {

    @Override
    public GlossMessage build(UpdateRow theUpdateRow) {
        Validate.notNull(theUpdateRow, "The update row cannot be null.");
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");
        String theTradeNumber = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        return new ExceptionGlossMessage(theExternalComments, theUserId, theTradeNumber, theStatusCode);
    }
}
