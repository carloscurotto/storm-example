package ar.com.carloscurotto.storm.complex.topology.propagator.executor;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.ResultRow;

public class ResultUpdatePropagationMessageResolver {

    public static String getMessage(final ResultRow theExternalResult, final ResultRow theInternalResult) {
        Validate.notNull(theExternalResult, "The external result cannot be null");
        Validate.notNull(theInternalResult, "The internal result cannot be null");
        String externalResultMessage = getStatusMessage(theExternalResult, "external");
        String internalResultMessage = getStatusMessage(theInternalResult, "internal");
        StringBuilder finalResultMessage = new StringBuilder();
        if (!theExternalResult.isFailure()) {
            return finalResultMessage.append("The ").append(internalResultMessage).append(" and the ").append(
                    externalResultMessage).toString();
        } else {
            return finalResultMessage.append("The ").append(externalResultMessage).toString();
        }
    }

    private static String getStatusMessage(final ResultRow theResult, final String theResultOriginName) {
        if (theResult.isSuccessful()) {
            return theResultOriginName + " result was successful";
        } else if (theResult.isFailure()) {
            return theResultOriginName + " failed with cause [" + theResult.getMessage() + "]";
        }
        return theResultOriginName + " was skipped";
    }
}
