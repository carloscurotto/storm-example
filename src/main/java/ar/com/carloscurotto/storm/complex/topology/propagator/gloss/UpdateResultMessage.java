package ar.com.carloscurotto.storm.complex.topology.propagator.gloss;

import org.apache.commons.lang3.Validate;

/**
 * Enum that tipifies the different update results messages.
 */
public enum UpdateResultMessage {
    SUCCESS("Success"), NO_MESSAGES("There were no messages to send"), TRADE_GLOSS_SYNC_ERROR(
            "The Trade was updated. Please wait for sometime and try again."), GLOSS_JMS_ERROR(
            "UnExpected Error occurred sending the Trade message to GLOSS. Please try again later.");

    /**
     * Update result message string.
     */
    private final String description;

    /**
     * Constructor.
     *
     * @param description
     *            a String
     */
    UpdateResultMessage(String description) {
        Validate.notBlank(description, "description cannot be blank");
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }
}