package cris.prs.messaging.solace.core;

/**
 * The Solace message exchange patterns, as a single declarative choice on {@code @SolaceListener}.
 *
 * <p>Each pattern is a policy over the individual endpoint knobs (endpoint mode, whether the
 * instance id is part of the endpoint name, and the endpoint's access type). Stating the intent once
 * is both clearer and harder to get wrong than setting three flags that have to agree with each
 * other: the difference between fan-out and competing consumers is exactly whether every instance
 * gets its own endpoint or they all share one.</p>
 *
 * <p>Anything set explicitly on the annotation still wins; the pattern only fills in what was left
 * unspecified.</p>
 */
public enum ExchangePattern {

    /**
     * Every consumer receives its own copy of every message.
     *
     * <p>Each application instance binds its own endpoint, named with the instance id, subscribed to
     * the same topic. Non-durable by default, so the broker removes an instance's endpoint when it
     * disconnects and a restarted pod does not inherit a backlog it never saw published.</p>
     */
    PUBLISH_SUBSCRIBE,

    /**
     * Each message is processed by exactly one consumer.
     *
     * <p>All instances bind to one shared, durable, non-exclusive endpoint and compete for messages
     * &mdash; Solace's consumer group form of point-to-point. Scaling the service out adds
     * throughput rather than duplicate processing.</p>
     */
    POINT_TO_POINT,

    /**
     * Point-to-point in both directions: a shared request endpoint, and a reply sent back to the
     * per-instance destination named in the request's {@code replyTo}.
     *
     * <p>This is the pattern {@code ReplyingSolaceTemplate} drives from the requesting side.</p>
     */
    REQUEST_REPLY
}
