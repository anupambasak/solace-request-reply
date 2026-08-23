package cris.prs.messaging.solace.listener;

/**
 * Callbacks a listener container invokes so that message handling can be measured.
 *
 * <p>Deliberately free of any metrics-library types, so the {@code listener} package stays
 * dependency-free and instrumentation is entirely optional. The Micrometer implementation lives in
 * {@code cris.prs.messaging.solace.observability}; when no implementation is supplied a container
 * uses {@link #NO_OP} and records nothing.</p>
 *
 * <p>Implementations are called on the thread that delivered the message, once per message, and must
 * therefore be cheap and must not throw. A container guards every call, so an exception is logged and
 * swallowed rather than failing the message &mdash; but an implementation that throws on every
 * message will fill the log.</p>
 */
public interface SolaceListenerMetrics {

    /** An implementation that records nothing. The default for every container. */
    SolaceListenerMetrics NO_OP = new SolaceListenerMetrics() {
    };

    /**
     * A message has been delivered to the container, before the listener is invoked.
     *
     * <p>Counting this separately from completion matters for {@code dispatch: EXECUTOR}, where a
     * message can be buffered for some time between the two.</p>
     *
     * @param listenerId the container's id
     */
    default void recordReceived(String listenerId) {
    }

    /**
     * The listener returned normally.
     *
     * @param listenerId    the container's id
     * @param durationNanos how long the listener invocation took
     */
    default void recordSuccess(String listenerId, long durationNanos) {
    }

    /**
     * The listener threw.
     *
     * @param listenerId    the container's id
     * @param durationNanos how long the listener invocation took before it failed
     * @param exception     what it threw; the exception type is a useful metric dimension
     */
    default void recordFailure(String listenerId, long durationNanos, Exception exception) {
    }
}
