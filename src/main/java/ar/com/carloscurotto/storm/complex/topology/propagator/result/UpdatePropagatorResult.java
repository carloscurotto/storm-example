package ar.com.carloscurotto.storm.complex.topology.propagator.result;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.model.ResultStatus;

public class UpdatePropagatorResult {

    private ResultStatus status;

    private String message;

    private UpdatePropagatorResult(final ResultStatus theStatus, final String theMessage) {
        Validate.notNull(theStatus, "The status cannot be null");
        Validate.notBlank(theMessage, "The message cannot be blank");
        status = theStatus;
        message = theMessage;
    }

    public static UpdatePropagatorResult createSuccess(final String theMessage) {
        return new UpdatePropagatorResult(ResultStatus.SUCCESS, theMessage);
    }

    public static UpdatePropagatorResult createFailure(final String theMessage) {
        return new UpdatePropagatorResult(ResultStatus.FAILURE, theMessage);
    }

    public ResultStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

}
