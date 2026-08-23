package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.SessionEvent;

/**
 * A lifecycle event on the JCSMP session, mapped from JCSMP's {@code SessionEvent}.
 *
 * <p>Session events sit one level below {@link SolaceFlowEvent}: the session is the TCP connection to
 * the broker, and every flow rides on one. JCSMP reconnects a session transparently, so a network
 * blip can interrupt every consumer and producer in the application and then repair itself without
 * anything else noticing &mdash; flows that survive the reconnect never raise a flow event.</p>
 *
 * <table border="1">
 *   <caption>What each event means</caption>
 *   <tr><th>Event</th><th>Meaning</th></tr>
 *   <tr><td>{@link #RECONNECTING}</td>
 *       <td>The connection dropped and JCSMP is retrying. <b>Nothing is being sent or received.</b></td></tr>
 *   <tr><td>{@link #RECONNECTED}</td><td>The connection is back and traffic has resumed.</td></tr>
 *   <tr><td>{@link #DOWN}</td>
 *       <td>The connection failed unrecoverably; JCSMP has stopped retrying. The application will not
 *           recover without a restart.</td></tr>
 *   <tr><td>{@link #SUBSCRIPTION_ERROR}</td>
 *       <td>The broker rejected a subscription added on the session. Direct consumers lose messages
 *           silently when this happens.</td></tr>
 *   <tr><td>{@link #VIRTUAL_ROUTER_NAME_CHANGED}</td>
 *       <td>The session reconnected to a <em>different</em> broker in an HA pair. Temporary endpoints
 *           and unacknowledged guaranteed messages do not survive this.</td></tr>
 *   <tr><td>{@link #INCOMPLETE_LARGE_MESSAGE}</td><td>A large message arrived truncated.</td></tr>
 *   <tr><td>{@link #UNKNOWN_TRANSACTED_SESSION}</td>
 *       <td>The broker does not recognise a transacted session this client believes it has &mdash;
 *           usually the aftermath of a failover.</td></tr>
 *   <tr><td>{@link #UNKNOWN}</td><td>A JCSMP event this library does not model.</td></tr>
 * </table>
 */
public enum SolaceSessionEvent {

    /** The connection dropped and JCSMP is retrying; nothing is being sent or received. */
    RECONNECTING,

    /** The connection is back and traffic has resumed. */
    RECONNECTED,

    /** The connection failed unrecoverably and JCSMP has stopped retrying. */
    DOWN,

    /** The broker rejected a subscription added on the session. */
    SUBSCRIPTION_ERROR,

    /** The session reconnected to a different broker in an HA pair. */
    VIRTUAL_ROUTER_NAME_CHANGED,

    /** A large message arrived truncated. */
    INCOMPLETE_LARGE_MESSAGE,

    /** The broker does not recognise a transacted session this client believes it has. */
    UNKNOWN_TRANSACTED_SESSION,

    /** A JCSMP event this library does not model. */
    UNKNOWN;

    /**
     * Map a JCSMP event.
     *
     * @param event the JCSMP event, or {@code null}
     * @return the corresponding constant, or {@link #UNKNOWN} for anything unrecognised
     */
    public static SolaceSessionEvent from(SessionEvent event) {
        if (event == null) {
            return UNKNOWN;
        }
        return switch (event) {
            case RECONNECTING -> RECONNECTING;
            case RECONNECTED -> RECONNECTED;
            case DOWN_ERROR -> DOWN;
            case SUBSCRIPTION_ERROR -> SUBSCRIPTION_ERROR;
            case VIRTUAL_ROUTER_NAME_CHANGED -> VIRTUAL_ROUTER_NAME_CHANGED;
            case INCOMPLETE_LARGE_MESSAGE_RECVD -> INCOMPLETE_LARGE_MESSAGE;
            case UNKNOWN_TRANSACTED_SESSION_NAME -> UNKNOWN_TRANSACTED_SESSION;
            // Exhaustive today; the default keeps a future JCSMP constant from throwing
            // IncompatibleClassChangeError here instead of arriving as UNKNOWN.
            default -> UNKNOWN;
        };
    }
}
