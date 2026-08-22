package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;

/** Strategy for dealing with an exception thrown by a listener. */
@FunctionalInterface
public interface SolaceListenerErrorHandler {

    /**
     * Deal with a listener failure.
     *
     * <p>Called after the container has already decided the message's fate, so this cannot change
     * whether the message is acknowledged or redelivered &mdash; it is for reporting, metrics or
     * routing a copy elsewhere.</p>
     *
     * @param message   the message whose listener threw
     * @param exception what it threw
     */
    void handleError(BytesXMLMessage message, Exception exception);
}
