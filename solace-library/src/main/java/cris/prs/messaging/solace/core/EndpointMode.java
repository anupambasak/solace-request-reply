package cris.prs.messaging.solace.core;

/**
 * How a consumer binds to the broker.
 *
 * <p>The three modes correspond to the three Solace delivery guarantees:</p>
 * <ul>
 *   <li>{@link #DURABLE_QUEUE} &mdash; guaranteed delivery, endpoint survives restarts.</li>
 *   <li>{@link #NON_DURABLE_QUEUE} &mdash; guaranteed delivery while the client is connected;
 *       the temporary endpoint is removed by the broker when the client disconnects.</li>
 *   <li>{@link #DIRECT} &mdash; non-persistent, at-most-once topic subscription with no endpoint,
 *       no acknowledgements and no transaction support.</li>
 * </ul>
 */
public enum EndpointMode {

    DURABLE_QUEUE,
    NON_DURABLE_QUEUE,
    DIRECT;

    public boolean isQueueBased() {
        return this != DIRECT;
    }
}
