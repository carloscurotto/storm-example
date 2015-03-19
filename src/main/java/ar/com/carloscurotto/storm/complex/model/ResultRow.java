package ar.com.carloscurotto.storm.complex.model;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class ResultRow {

    private String id;

    private String message;

    private ResultStatus status;

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public ResultRow() {
    }

    private ResultRow(final String theId, final ResultStatus theStatus, final String theMessage) {
        Validate.notBlank(theId, "The id can not be null.");
        Validate.notNull(theStatus, "The status can not be null.");
        id = theId;
        message = theMessage;
        status = theStatus;
    }

    public static ResultRow from(final String theId,
            final UpdatePropagatorResult theUpdatePropagatorResult) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notNull(theUpdatePropagatorResult, "The update propagator result.");
        return new ResultRow(theId, theUpdatePropagatorResult.getStatus(),
                theUpdatePropagatorResult.getMessage());
    }

    public static ResultRow skip(final String theId) {
        Validate.notBlank(theId, "The id can not be blank.");
        return new ResultRow(theId, ResultStatus.SKIPPED, null);
    }

    public static ResultRow success(final String theId, final String theMessage) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notBlank(theId, "The message can not be blank.");
        return new ResultRow(theId, ResultStatus.SUCCESS, theMessage);
    }

    public static ResultRow failure(final String theId, final String theMessage) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notBlank(theId, "The message can not be blank.");
        return new ResultRow(theId, ResultStatus.FAILURE, theMessage);
    }

    public String getId() {
        return id;
    }

    public String getMessage() {
        return message;
    }

    public boolean isSuccessful() {
        return status == ResultStatus.SUCCESS;
    }

    public boolean isFailure() {
        return status == ResultStatus.FAILURE;
    }

    public boolean isSkipped() {
        return status == ResultStatus.SKIPPED;
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof ResultRow) {
            final ResultRow other = (ResultRow) object;
            return Objects.equal(id, other.id) && Objects.equal(message, other.message)
                    && Objects.equal(status, other.status);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, message, status);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("message", message)
                .add("status", status).toString();
    }

}
