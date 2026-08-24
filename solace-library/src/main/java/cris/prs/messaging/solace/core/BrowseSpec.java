package cris.prs.messaging.solace.core;

import lombok.Data;

import java.time.Duration;

/**
 * Describes a queue browse.
 *
 * <p>Browsing reads messages from a queue <strong>without consuming them</strong>: the messages stay
 * spooled and are still delivered to whatever consumer is bound. It is the operator's view of a
 * queue &mdash; what is sitting on the dead message queue, why a backlog is not draining, what a
 * poison message actually contains.</p>
 *
 * <p>A browser binds to the endpoint like a consumer does, so it counts against the endpoint's bind
 * limit. An <b>exclusive</b> endpoint that already has its one consumer will reject a browser, and a
 * <b>non-durable</b> queue belongs to the client that created it and cannot be browsed from
 * elsewhere. In practice browsing is for durable queues, and the dead message queue above all.</p>
 */
@Data
public class BrowseSpec {

    /** Create a browse specification with every value at its documented default. */
    public BrowseSpec() {
    }

    /**
     * Name of the queue to browse.
     *
     * <p>A queue name, not a topic: browsing reads a spooled endpoint, and a topic spools nothing.
     * The dead message queue is {@code #DEAD_MSG_QUEUE}.</p>
     */
    private String queue;

    /**
     * Optional broker-side selector, filtering what the browse returns.
     *
     * <p>Evaluated by the broker, so an unmatched message is never transferred &mdash; which is what
     * makes it usable on a queue with a large backlog.</p>
     */
    private String selector;

    /**
     * How long {@code next()} waits for a message before giving up.
     *
     * <p>Zero means do not wait at all, which is what a "drain what is there now" loop wants: a
     * browser at the end of a queue would otherwise block for this long on every pass.</p>
     */
    private Duration waitTimeout = Duration.ZERO;

    /**
     * Messages the broker may have in flight to the browser.
     *
     * <p>Unset uses the JCSMP default. Raising it speeds up a long browse and costs memory while the
     * browse runs.</p>
     */
    private Integer transportWindowSize;

    /**
     * Build a specification for one queue, with every other value defaulted.
     *
     * @param queue the queue to browse
     * @return the specification
     */
    public static BrowseSpec of(String queue) {
        BrowseSpec spec = new BrowseSpec();
        spec.setQueue(queue);
        return spec;
    }

    /**
     * Build a specification for one queue with a selector.
     *
     * @param queue    the queue to browse
     * @param selector a broker-side selector over message properties
     * @return the specification
     */
    public static BrowseSpec of(String queue, String selector) {
        BrowseSpec spec = of(queue);
        spec.setSelector(selector);
        return spec;
    }
}
