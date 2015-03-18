package ar.com.carloscurotto.storm.complex.model;

import org.apache.commons.lang3.Validate;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class ResultRow {

    private String rowId;

    private ResultRowStatus status;

    private String message;

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public ResultRow() {
    }

    public ResultRow(final String theRowId, final ResultRowStatus theStatus, final String theMessage) {
        Validate.notBlank(theRowId, "The row id can not be blank.");
        Validate.notNull(theStatus, "The status can not be null");
        Validate.notBlank(theMessage, "The message can not be blank");
        rowId = theRowId;
        status = theStatus;
        message = theMessage;
    }

    public String getRowId() {
        return rowId;
    }

    public boolean isSuccessful() {
        return status == ResultRowStatus.SUCCESS;
    }

    public boolean isFailure() {
        return status == ResultRowStatus.FAILURE;
    }

    public boolean isSkipped() {
        return status == ResultRowStatus.SKIPPED;
    }

    public ResultRowStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof ResultRow) {
            final ResultRow other = (ResultRow) object;
            return Objects.equal(rowId, other.rowId) && Objects.equal(status, other.status)
                    && Objects.equal(message, other.message);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rowId, status, message);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", rowId).add("status", status).add("message", message).toString();
    }

}
