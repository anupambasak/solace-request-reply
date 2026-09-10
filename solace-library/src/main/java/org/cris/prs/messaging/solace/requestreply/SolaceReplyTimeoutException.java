package org.cris.prs.messaging.solace.requestreply;

import org.cris.prs.messaging.solace.core.SolaceMessagingException;

/** Thrown into the future when no reply arrives within the configured timeout. */
public class SolaceReplyTimeoutException extends SolaceMessagingException {

    /**
     * Create a timeout exception.
     *
     * @param message which correlation id timed out, and after how long
     */
    public SolaceReplyTimeoutException(String message) {
        super(message);
    }
}
