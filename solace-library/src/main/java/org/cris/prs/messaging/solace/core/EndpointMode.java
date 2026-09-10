package org.cris.prs.messaging.solace.core;

/**
 * How a consumer binds to the broker.
 *
 * <p>The three modes are the three Solace delivery guarantees. The choice is independent of the
 * {@link ExchangePattern}: a pattern decides <em>who</em> receives a message, a mode decides
 * <em>how well</em> its delivery is guaranteed.</p>
 */
public enum EndpointMode {

    /**
     * Guaranteed delivery on an endpoint that survives restarts.
     *
     * <p>The queue is provisioned if missing and keeps its messages and subscriptions between runs.
     * The natural choice for competing consumers, and the only one where a message can outlive the
     * consumer that was meant to handle it &mdash; so also the only one needing a dead message queue.</p>
     */
    DURABLE_QUEUE,

    /**
     * Guaranteed delivery for as long as the client stays connected.
     *
     * <p>A temporary endpoint is created when a flow binds to it and removed by the broker when the
     * client disconnects. Ideal for per-instance destinations such as a reply endpoint: nothing has
     * to be cleaned up when a pod is replaced, and a restarted pod does not inherit a backlog it
     * never asked for.</p>
     */
    NON_DURABLE_QUEUE,

    /**
     * At-most-once delivery with no endpoint at all: a plain topic subscription.
     *
     * <p>The lowest latency option, and the only one with no acknowledgement, no redelivery and no
     * transaction support. Messages published while the consumer is disconnected are lost.</p>
     */
    DIRECT;

    /**
     * Whether this mode binds to a queue endpoint rather than subscribing the session directly.
     *
     * <p>Acknowledgement, redelivery and transactions all require a queue, so this is the test for
     * whether those features are available.</p>
     *
     * @return {@code true} for the queue-based modes, {@code false} for {@link #DIRECT}
     */
    public boolean isQueueBased() {
        return this != DIRECT;
    }
}
