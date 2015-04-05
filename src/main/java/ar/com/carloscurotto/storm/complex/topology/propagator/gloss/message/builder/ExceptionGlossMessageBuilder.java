package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.ExceptionGlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;

public class ExceptionGlossMessageBuilder extends GlossMessageBuilder {

    @Override
    protected GlossMessage doBuild(UpdateRow theUpdateRow) {
        Validate.notNull(theUpdateRow, "The update row cannot be null.");
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");
        String theTradeNumber = (String) theUpdateRow.getUpdateColumnValue("tradeNo");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        return new ExceptionGlossMessage(theExternalComments, theUserId, theTradeNumber, theStatusCode);
    }

    @Override
    protected boolean shouldBuild(final Map<String, Object> parameters) {
        return isParameterValueTrue(parameters, "update") && isParameterValueTrue(parameters, "exceptionTrade");
    }
}
