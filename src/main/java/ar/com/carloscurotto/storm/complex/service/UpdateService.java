package ar.com.carloscurotto.storm.complex.service;

import org.apache.commons.lang.Validate;

import ar.com.carloscurotto.storm.complex.model.Result;
import ar.com.carloscurotto.storm.complex.model.Update;

/**
 * This class represents the main class of our update service. Clients will use this class to execute updates against
 * our system.
 *
 * @author O605461, W506376
 */
public class UpdateService extends OpenAwareSubmitter<Update, Result> {

    private OpenAwareSubmitter<Update, Result> submitter;

    /**
     * Creates an update service.
     *
     * @param theSubmitter
     *            the submitter to use when executing updates. It can not be null.
     */
    public UpdateService(final OpenAwareSubmitter<Update, Result> theSubmitter) {
        Validate.notNull(theSubmitter, "The submitter can not be null.");
        submitter = theSubmitter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doOpen() {
        submitter.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doClose() {
        submitter.close();
    }

    /**
     * Performs the update.
     *
     * @param update
     *            the {@link Update} to process. It cannot be null.
     * @return a {@link Result}. It may be null.
     */
    @Override
    public Result doSubmit(final Update theUpdate) {
        Validate.notNull(theUpdate, "The update cannot be null");
        return submitter.submit(theUpdate);
    }
}
