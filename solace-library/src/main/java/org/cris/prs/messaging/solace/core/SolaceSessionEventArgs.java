package org.cris.prs.messaging.solace.core;

import lombok.Getter;
import lombok.ToString;

/**
 * Everything known about one session lifecycle event.
 *
 * <p>A value object rather than a widened callback signature, so a future JCSMP event field can be
 * added without breaking every {@link SolaceSessionListener} implementation.</p>
 */
@Getter
@ToString
public class SolaceSessionEventArgs {

    /** What happened. */
    private final SolaceSessionEvent event;

    /** The connection state after this event. */
    private final SolaceSessionState state;

    /** JCSMP's description of the event, or {@code null}. */
    private final String info;

    /** The cause, for a failure event; {@code null} otherwise. */
    private final Exception exception;

    /** The broker's response code, or {@code 0} when the event carried none. */
    private final int responseCode;

    /**
     * Create the event arguments.
     *
     * @param event        what happened
     * @param state        the connection state after this event
     * @param info         JCSMP's description of the event, or {@code null}
     * @param exception    the cause for a failure event, or {@code null}
     * @param responseCode the broker's response code, or {@code 0}
     */
    public SolaceSessionEventArgs(SolaceSessionEvent event, SolaceSessionState state, String info,
            Exception exception, int responseCode) {
        this.event = event;
        this.state = state;
        this.info = info;
        this.exception = exception;
        this.responseCode = responseCode;
    }
}
