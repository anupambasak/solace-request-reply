package cris.prs.messaging.solace.core;

import org.springframework.core.NestedRuntimeException;

/** Unchecked wrapper for the checked {@code JCSMPException} hierarchy. */
public class SolaceMessagingException extends NestedRuntimeException {

    public SolaceMessagingException(String message) {
        super(message);
    }

    public SolaceMessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
