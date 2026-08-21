package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;

/** Strategy for dealing with an exception thrown by a listener. */
@FunctionalInterface
public interface SolaceListenerErrorHandler {

    void handleError(BytesXMLMessage message, Exception exception);
}
