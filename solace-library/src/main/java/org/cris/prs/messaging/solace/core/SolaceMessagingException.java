package org.cris.prs.messaging.solace.core;

import org.springframework.core.NestedRuntimeException;

/**
 * Unchecked wrapper for the checked {@code JCSMPException} hierarchy.
 *
 * <p>Extends Spring's {@code NestedRuntimeException}, so {@code getMostSpecificCause()} reaches the
 * underlying JCSMP failure &mdash; usually a {@code JCSMPErrorResponseException} whose
 * {@code getSubcodeEx()} identifies precisely what the broker objected to.</p>
 */
public class SolaceMessagingException extends NestedRuntimeException {

    /**
     * Create an exception with no underlying cause.
     *
     * @param message description of what the library was attempting
     */
    public SolaceMessagingException(String message) {
        super(message);
    }

    /**
     * Wrap an underlying messaging failure.
     *
     * @param message description of what the library was attempting
     * @param cause   the underlying failure, typically a {@code JCSMPException}
     */
    public SolaceMessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
