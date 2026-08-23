package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.FlowEvent;

/**
 * A lifecycle event on a consumer flow, mapped from JCSMP's {@code FlowEvent}.
 *
 * <p>A flow is a consumer's binding to an endpoint. Its lifecycle is invisible from the outside
 * &mdash; a flow can go down and come back without a single message being lost or a single log line
 * appearing &mdash; which is why these events matter operationally: they are the only place a
 * reconnect, a lost bind, or a change of active consumer becomes observable.</p>
 *
 * <table border="1">
 *   <caption>What each event means</caption>
 *   <tr><th>Event</th><th>Meaning</th></tr>
 *   <tr><td>{@link #UP}</td><td>The flow bound successfully and is consuming.</td></tr>
 *   <tr><td>{@link #DOWN}</td>
 *       <td>The flow was lost and will not come back on its own &mdash; the endpoint was deleted, the
 *           bind was rejected, or an unrecoverable error occurred. <b>The container will not consume
 *           again until it is restarted.</b></td></tr>
 *   <tr><td>{@link #RECONNECTING}</td>
 *       <td>The flow was lost and JCSMP is retrying. Consumption has stopped for now; this is the
 *           state a health check should report as degraded rather than up.</td></tr>
 *   <tr><td>{@link #RECONNECTED}</td><td>The retry succeeded and consumption has resumed.</td></tr>
 *   <tr><td>{@link #ACTIVE}</td>
 *       <td>This flow is <b>the</b> consumer on an exclusive endpoint. Only delivered when active
 *           flow indication is enabled.</td></tr>
 *   <tr><td>{@link #INACTIVE}</td>
 *       <td>This flow is standing by; another instance holds the exclusive endpoint.</td></tr>
 *   <tr><td>{@link #UNKNOWN}</td>
 *       <td>A JCSMP event this library does not model. Reported rather than swallowed, so a client
 *           upgrade that adds an event does not silently drop it.</td></tr>
 * </table>
 */
public enum SolaceFlowEvent {

    /** The flow bound successfully and is consuming. */
    UP,

    /** The flow was lost and will not recover without a container restart. */
    DOWN,

    /** The flow was lost and JCSMP is retrying; consumption has stopped for now. */
    RECONNECTING,

    /** A retry succeeded and consumption has resumed. */
    RECONNECTED,

    /** This flow is the active consumer on an exclusive endpoint. */
    ACTIVE,

    /** This flow is standing by; another instance holds the exclusive endpoint. */
    INACTIVE,

    /** A JCSMP event this library does not model. */
    UNKNOWN;

    /**
     * Map a JCSMP event.
     *
     * @param event the JCSMP event, or {@code null}
     * @return the corresponding constant, or {@link #UNKNOWN} for anything unrecognised
     */
    public static SolaceFlowEvent from(FlowEvent event) {
        if (event == null) {
            return UNKNOWN;
        }
        return switch (event) {
            case FLOW_UP -> UP;
            case FLOW_DOWN -> DOWN;
            case FLOW_RECONNECTING -> RECONNECTING;
            case FLOW_RECONNECTED -> RECONNECTED;
            case FLOW_ACTIVE -> ACTIVE;
            case FLOW_INACTIVE -> INACTIVE;
            // Exhaustive today; the default is what keeps a future JCSMP constant from throwing
            // IncompatibleClassChangeError here instead of arriving as UNKNOWN.
            default -> UNKNOWN;
        };
    }

    /**
     * Whether this event means the flow is not currently delivering messages.
     *
     * <p>{@link #INACTIVE} is deliberately excluded: a standby flow on an exclusive endpoint is
     * healthy and working as designed, and reporting it as degraded would fail the health check of
     * every instance that is not the leader.</p>
     *
     * @return {@code true} for {@link #DOWN} and {@link #RECONNECTING}
     */
    public boolean isDegraded() {
        return this == DOWN || this == RECONNECTING;
    }
}
