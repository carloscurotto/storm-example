package ar.com.carloscurotto.storm.complex.service;

import ar.com.carloscurotto.storm.complex.Result;
import ar.com.carloscurotto.storm.complex.Update;
import ar.com.carloscurotto.storm.complex.transport.Submitter;

import com.google.common.base.Preconditions;

/**
 * This class represents the main class of our update service. Clients will use this class to execute updates against
 * our system.
 *
 * @author O605461
 */
public class UpdateService extends OpenAwareService<Update, Result> {

    private Submitter<Update, Result> submitter;

    /**
     * Creates an update service.
     *
     * @param theSubmitter
     *            the submitter to use when executing updates. It can not be null.
     */
    public UpdateService(final Submitter<Update, Result> theSubmitter) {
        Preconditions.checkArgument(theSubmitter != null, "The submitter can not be null.");
        submitter = theSubmitter;
    }

    @Override
    protected void doOpen() {
        submitter.open();
    }

    @Override
    protected void doClose() {
        submitter.close();
    }

    /**
     * Performs the actual update.
     *
     * @param update
     *            the update to process. It can not be null.
     */
    protected Result doExecute(final Update theUpdate) {
        Preconditions.checkArgument(theUpdate != null, "The update can not be null.");
        return submitter.submit(theUpdate);
    }
}
