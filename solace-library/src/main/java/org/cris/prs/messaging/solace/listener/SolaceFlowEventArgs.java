package org.cris.prs.messaging.solace.listener;

import org.cris.prs.messaging.solace.core.SolaceFlowEvent;
import lombok.Getter;
import lombok.ToString;

/**
 * Everything known about one flow lifecycle event.
 *
 * <p>A value object rather than a widened callback signature, so a future JCSMP event field can be
 * added without breaking every {@link SolaceFlowListener} implementation.</p>
 */
@Getter
@ToString
public class SolaceFlowEventArgs {

    /** Id of the container whose flow raised the event. */
    private final String listenerId;

    /** Which of the container's flows raised it, counting from zero. */
    private final int flowIndex;

    /** The physical endpoint the flow is bound to, or {@code null} before it resolved. */
    private final String endpoint;

    /** What happened. */
    private final SolaceFlowEvent event;

    /** JCSMP's description of the event, or {@code null}. */
    private final String info;

    /** The cause, for a failure event; {@code null} otherwise. */
    private final Exception exception;

    /** The broker's response code, or {@code 0} when the event carried none. */
    private final int responseCode;

    /**
     * Create the event arguments.
     *
     * @param listenerId   id of the container whose flow raised the event
     * @param flowIndex    which of the container's flows raised it, counting from zero
     * @param endpoint     the physical endpoint the flow is bound to, or {@code null}
     * @param event        what happened
     * @param info         JCSMP's description of the event, or {@code null}
     * @param exception    the cause for a failure event, or {@code null}
     * @param responseCode the broker's response code, or {@code 0}
     */
    public SolaceFlowEventArgs(String listenerId, int flowIndex, String endpoint,
            SolaceFlowEvent event, String info, Exception exception, int responseCode) {
        this.listenerId = listenerId;
        this.flowIndex = flowIndex;
        this.endpoint = endpoint;
        this.event = event;
        this.info = info;
        this.exception = exception;
        this.responseCode = responseCode;
    }
}
