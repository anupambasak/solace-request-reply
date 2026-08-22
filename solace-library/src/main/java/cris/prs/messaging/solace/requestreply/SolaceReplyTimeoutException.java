package cris.prs.messaging.solace.requestreply;

import cris.prs.messaging.solace.core.SolaceMessagingException;

/** Thrown into the future when no reply arrives within the configured timeout. */
public class SolaceReplyTimeoutException extends SolaceMessagingException {

    /**
     * @param message which correlation id timed out, and after how long
     */
    public SolaceReplyTimeoutException(String message) {
        super(message);
    }
}
