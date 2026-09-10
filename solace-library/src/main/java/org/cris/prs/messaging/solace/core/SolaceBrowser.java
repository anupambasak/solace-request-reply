package org.cris.prs.messaging.solace.core;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reads messages from a queue without consuming them.
 *
 * <p>A browser is a cursor over what is spooled on an endpoint <em>right now</em>. Reading does not
 * acknowledge, so every message stays on the queue and is still delivered to whatever consumer is
 * bound to it. That is the whole point: it is the only way to look at a dead message queue, or at a
 * backlog that is not draining, without taking the messages away from the thing that should process
 * them.</p>
 *
 * <p>It is <strong>not</strong> a consumer, and not a substitute for one. There is no
 * acknowledgement, no redelivery, no transaction, and no notification of new arrivals &mdash; a
 * browser sees the queue as it was while it walks it.</p>
 *
 * <p>Always close it. A browser holds a bind on the endpoint, which counts against that endpoint's
 * bind limit, so an unclosed browser can keep a consumer from binding:</p>
 *
 * <pre>{@code
 * try (SolaceBrowser<Order> browser = solace.browse("#DEAD_MSG_QUEUE", Order.class)) {
 *     browser.stream(100).forEach(record ->
 *             log.info("dead: {} after {} deliveries", record.getPayload(), record.getDeliveryCount()));
 * }
 * }</pre>
 *
 * <p>Not thread-safe: one browser belongs to one thread.</p>
 *
 * @param <T> the type message bodies are converted into
 */
public interface SolaceBrowser<T> extends AutoCloseable {

    /**
     * Read the next message, waiting up to the specification's {@code waitTimeout}.
     *
     * <p>With the default zero timeout this does not block: an empty result means "nothing available
     * right now", which at the end of a queue is the normal way to stop.</p>
     *
     * @return the next message, or empty if none arrived within the timeout
     */
    Optional<SolaceRecord<T>> next();

    /**
     * Read up to {@code max} messages.
     *
     * <p>Stops early when the queue runs out, so a shorter list than asked for means the browse
     * reached the end rather than that anything failed.</p>
     *
     * @param max the most to read; must be positive
     * @return the messages read, in queue order, possibly empty
     */
    List<SolaceRecord<T>> take(int max);

    /**
     * Walk the queue as a lazy stream, ending when it runs out of messages.
     *
     * <p>Unbounded: on a queue that is still being published to, this can run for a long time. Prefer
     * {@link #stream(int)} unless the queue is known to be static.</p>
     *
     * @return a lazy stream over the remaining messages
     */
    Stream<SolaceRecord<T>> stream();

    /**
     * Walk at most {@code limit} messages as a lazy stream.
     *
     * @param limit the most to read; must be positive
     * @return a lazy stream over up to {@code limit} messages
     */
    Stream<SolaceRecord<T>> stream(int limit);

    /**
     * Delete a browsed message from the queue.
     *
     * <p><strong>Destructive, and the one operation here that is not read-only.</strong> The message
     * is removed from the endpoint and no consumer will ever see it. It is the reason browsing a dead
     * message queue is useful operationally &mdash; inspect a poison message, then drop it &mdash;
     * and the reason a browse should not be pointed at a live work queue by accident.</p>
     *
     * @param record a record returned by this browser
     */
    void remove(SolaceRecord<T> record);

    /**
     * Release the bind on the endpoint.
     *
     * <p>Idempotent, and does not throw a checked exception, so a try-with-resources block needs no
     * catch.</p>
     */
    @Override
    void close();
}
