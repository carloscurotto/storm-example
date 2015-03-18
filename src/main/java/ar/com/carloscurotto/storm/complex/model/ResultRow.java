package ar.com.carloscurotto.storm.complex.model;

import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.result.UpdatePropagatorResult;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class ResultRow {

    private String rowId;

    private String message;

    private ResultStatus status;

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public ResultRow() {
    }

    private ResultRow(final String theRowId, final ResultStatus theStatus, final String theMessage) {
        Validate.notBlank(theRowId, "The row id can not be null.");
        Validate.notNull(theStatus, "The status can not be null.");
        rowId = theRowId;
        message = theMessage;
        status = theStatus;
    }

    public static ResultRow from(final String theRowId,
            final UpdatePropagatorResult theUpdatePropagatorResult) {
        Validate.notBlank(theRowId, "The row id can not be blank.");
        Validate.notNull(theUpdatePropagatorResult, "The update propagator result.");
        return new ResultRow(theRowId, theUpdatePropagatorResult.getStatus(),
                theUpdatePropagatorResult.getMessage());
    }

    public static ResultRow skip(final String theRowId) {
        Validate.notBlank(theRowId, "The row id can not be blank.");
        return new ResultRow(theRowId, ResultStatus.SKIPPED, null);
    }

    public static ResultRow success(final String theRowId, final String theMessage) {
        Validate.notBlank(theRowId, "The row id can not be blank.");
        Validate.notBlank(theRowId, "The message can not be blank.");
        return new ResultRow(theRowId, ResultStatus.SUCCESS, theMessage);
    }

    public static ResultRow failure(final String theRowId, final String theMessage) {
        Validate.notBlank(theRowId, "The row id can not be blank.");
        Validate.notBlank(theRowId, "The message can not be blank.");
        return new ResultRow(theRowId, ResultStatus.FAILURE, theMessage);
    }

    public String getRowId() {
        return rowId;
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
            return Objects.equal(rowId, other.rowId) && Objects.equal(message, other.message)
                    && Objects.equal(status, other.status);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rowId, message, status);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", rowId).add("message", message)
                .add("status", status).toString();
    }

}
