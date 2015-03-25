package ar.com.carloscurotto.storm.complex.model;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import ar.com.carloscurotto.storm.complex.topology.propagator.executor.AbstractUpdatePropagatorExecutor;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * This class models a single result for a particular {@link UpdateRow} that is returned by the different
 * {@link AbstractUpdatePropagatorExecutor} .
 *
 * @author O605461, W506376
 */
public class ResultRow implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The identifier that is associated with the {@link UpdateRow}
     */
    private String id;

    /**
     * The message that is sent back to the client. It may be null or empty.
     */
    private String message;

    /**
     * The final status of this result.
     */
    private ResultStatus status;

    /**
     * Constructor. Do not use, only for serialization purposes.
     */
    @Deprecated
    public ResultRow() {
    }

    private ResultRow(final String theId, final ResultStatus theStatus, final String theMessage) {
        Validate.notNull(theStatus, "The status can not be null.");
        id = theId;
        message = theMessage;
        status = theStatus;
    }

    /**
     * Constructs an instance with the given parameters. This result is not yet associated with any identifier.
     *
     * @param theStatus
     *            the {@link ResultStatus} for this result. It cannot be null.
     * @param theMessage
     *            the message for this result. It may be null or empty.
     */
    public ResultRow(final ResultStatus theStatus, final String theMessage) {
        this(null, theStatus, theMessage);
    }

    /**
     * Creates a new instance of {@link ResultRow} with a {@link ResultStatus#SKIPPED} status.
     *
     * @param theId
     *            the identifier for this result that is associated with its input data. It cannot be blank.
     * @return a new instance of {@link ResultRow}. It is never null
     */
    public static ResultRow skip(final String theId) {
        Validate.notBlank(theId, "The id can not be blank.");
        return new ResultRow(theId, ResultStatus.SKIPPED, null);
    }

    /**
     * Creates a new instance of {@link ResultRow} with a {@link ResultStatus#SUCCESS} status.
     *
     * @param theId
     *            the identifier for this result that is associated with its input data. It cannot be blank.
     * @param theMessage
     *            the message for this result. It cannot be blank.
     * @return a new instance of {@link ResultRow}. It is never null
     */
    public static ResultRow success(final String theId, final String theMessage) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notBlank(theMessage, "The message can not be blank.");
        return new ResultRow(theId, ResultStatus.SUCCESS, theMessage);
    }

    /**
     * Creates a new instance of {@link ResultRow} with a {@link ResultStatus#FAILURE} status.
     *
     * @param theId
     *            the identifier for this result that is associated with its input data. It cannot be blank.
     * @param theMessage
     *            the message for this result. It cannot be blank.
     * @return a new instance of {@link ResultRow}. It is never null
     */
    public static ResultRow failure(final String theId, final String theMessage) {
        Validate.notBlank(theId, "The id can not be blank.");
        Validate.notBlank(theMessage, "The message can not be blank.");
        return new ResultRow(theId, ResultStatus.FAILURE, theMessage);
    }

    /**
     * The identifier that is associated with the {@link UpdateRow}.
     *
     * @return the identifier. It may be null if this association was not made.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the message of this result.
     *
     * @return the message of this result. It may be null.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Checks whether this is a successfully result.
     *
     * @return true if this result was success. False otherwise.
     */
    public boolean isSuccessful() {
        return status == ResultStatus.SUCCESS;
    }

    /**
     * Checks whether this is failure result.
     *
     * @return true if this result was failed. False otherwise.
     */
    public boolean isFailure() {
        return status == ResultStatus.FAILURE;
    }

    /**
     * Checks whether this is a skipped result.
     *
     * @return true if this result was skipepd. False otherwise.
     */
    public boolean isSkipped() {
        return status == ResultStatus.SKIPPED;
    }

    /**
     * Sets the identifier for this result.
     *
     * @param theId
     *            the identifier for this result. It cannot be blank.
     * @throws {@link IllegalStateException} if this result was already associated with an identifier.
     */
    public void setId(final String theId) {
        Validate.notEmpty(theId, "The id cannot be blank");
        Preconditions.checkState(StringUtils.isBlank(id), "The id for this result has been already set");
        id = theId;
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
        return MoreObjects.toStringHelper(this).add("id", id).add("message", message).add("status", status).toString();
    }

}
