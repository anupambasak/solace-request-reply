package cris.prs.messaging.solace.core;

/**
 * The connection state of a {@link SolaceSessionFactory}, for health reporting.
 *
 * <p>Deliberately more than a boolean: a session that is <em>reconnecting</em> is neither healthy nor
 * permanently broken, and a readiness probe needs to tell those apart. Reporting a reconnect as
 * {@link #DOWN} would restart-loop a pod through a network blip; reporting it as {@link #CONNECTED}
 * would keep it taking traffic it cannot serve.</p>
 */
public enum SolaceSessionState {

    /**
     * No session has been created yet.
     *
     * <p>Healthy: an application that has not needed the broker is not broken, and reporting it as
     * down would fail the readiness probe of anything that connects lazily.</p>
     */
    NOT_CONNECTED(true),

    /** Connected and usable. */
    CONNECTED(true),

    /** The connection dropped and JCSMP is retrying. Nothing is being sent or received. */
    RECONNECTING(false),

    /** The session is closed or failed unrecoverably. */
    DOWN(false);

    private final boolean healthy;

    SolaceSessionState(boolean healthy) {
        this.healthy = healthy;
    }

    /**
     * Whether this state should be reported as healthy.
     *
     * @return {@code true} for {@link #NOT_CONNECTED} and {@link #CONNECTED}
     */
    public boolean isHealthy() {
        return this.healthy;
    }
}
