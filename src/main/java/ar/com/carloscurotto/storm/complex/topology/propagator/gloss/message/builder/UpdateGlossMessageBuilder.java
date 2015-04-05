package ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.builder;

import java.util.Map;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.UpdateRow;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.GlossMessage;
import ar.com.carloscurotto.storm.complex.topology.propagator.gloss.message.UpdateGlossMessage;

public class UpdateGlossMessageBuilder extends GlossMessageBuilder {

    @Override
    protected GlossMessage doBuild(UpdateRow theUpdateRow) {
        Validate.notNull(theUpdateRow, "The update row cannot be null.");
        Long theInstNumber = (Long) theUpdateRow.getUpdateColumnValue("instNumber");
        String theStatusCode = (String) theUpdateRow.getUpdateColumnValue("statusCode");
        String theService = (String) theUpdateRow.getUpdateColumnValue("service");
        String theExternalComments = (String) theUpdateRow.getUpdateColumnValue("externalComments");
        String theUserId = (String) theUpdateRow.getUpdateColumnValue("userId");
        String theTradeNumber = (String) theUpdateRow.getUpdateColumnValue("tradeNo");

        return new UpdateGlossMessage(theInstNumber, theStatusCode, theService, theExternalComments, theUserId,
                theTradeNumber);
    }

    @Override
    protected boolean shouldBuild(Map<String, Object> parameters) {
        return isParameterValueTrue(parameters, "update") && !isParameterValueTrue(parameters, "exceptionTrade");
    }
}
