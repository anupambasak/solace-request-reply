package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link SolaceBrowser} over a JCSMP {@code Browser}.
 *
 * <p>Conversion and header mapping are the same collaborators the template and the listener
 * containers use, so a browsed record is indistinguishable from a consumed one apart from never
 * having been acknowledged.</p>
 *
 * @param <T> the type message bodies are converted into
 */
@Slf4j
public class DefaultSolaceBrowser<T> implements SolaceBrowser<T> {

    private final Browser browser;

    private final SolaceMessageConverter messageConverter;

    private final SolaceHeaderMapper headerMapper;

    private final Class<T> payloadType;

    private final Duration waitTimeout;

    private final String queue;

    private volatile boolean closed;

    /**
     * Create a browser.
     *
     * @param browser          the JCSMP browser to read through
     * @param messageConverter converts message bodies
     * @param headerMapper     maps message fields and user properties
     * @param payloadType      the type bodies are converted into
     * @param waitTimeout      how long {@link #next()} waits; zero does not block
     * @param queue            the queue being browsed, for log messages
     */
    public DefaultSolaceBrowser(Browser browser, SolaceMessageConverter messageConverter,
            SolaceHeaderMapper headerMapper, Class<T> payloadType, Duration waitTimeout, String queue) {
        this.browser = browser;
        this.messageConverter = messageConverter;
        this.headerMapper = headerMapper;
        this.payloadType = payloadType;
        this.waitTimeout = waitTimeout == null ? Duration.ZERO : waitTimeout;
        this.queue = queue;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Uses {@code getNextNoWait()} for a zero timeout rather than {@code getNext(0)}, because
     * JCSMP reads zero as "wait forever" &mdash; which would hang at the end of every queue.</p>
     */
    @Override
    @SuppressWarnings("unchecked")
    public Optional<SolaceRecord<T>> next() {
        if (this.closed) {
            return Optional.empty();
        }
        try {
            BytesXMLMessage message = this.waitTimeout.isZero() || this.waitTimeout.isNegative()
                    ? this.browser.getNextNoWait()
                    : this.browser.getNext((int) this.waitTimeout.toMillis());
            if (message == null) {
                return Optional.empty();
            }
            Map<String, Object> headers = this.headerMapper.toHeaders(message);
            headers.put(SolaceHeaders.RAW_MESSAGE, message);
            Object payload = this.messageConverter.fromMessage(message, this.payloadType);
            return Optional.of(new SolaceRecord<>((T) payload,
                    message.getDestination() != null ? message.getDestination().getName() : null,
                    message.getCorrelationId(),
                    message.getReplyTo() != null ? message.getReplyTo().getName() : null,
                    headers, message, DefaultSolaceHeaderMapper.deliveryCountOf(message)));
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to browse queue '" + this.queue + "'", ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<SolaceRecord<T>> take(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("'max' must be positive");
        }
        List<SolaceRecord<T>> records = new ArrayList<>(Math.min(max, 128));
        for (int i = 0; i < max; i++) {
            Optional<SolaceRecord<T>> record = next();
            if (record.isEmpty()) {
                break;
            }
            records.add(record.get());
        }
        return records;
    }

    /** {@inheritDoc} */
    @Override
    public Stream<SolaceRecord<T>> stream() {
        return stream(Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Lazy: the iterator reads one message at a time, so a short-circuiting terminal operation
     * such as {@code findFirst} or {@code limit} stops the browse rather than draining the queue
     * first.</p>
     */
    @Override
    public Stream<SolaceRecord<T>> stream(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("'limit' must be positive");
        }
        Iterator<SolaceRecord<T>> iterator = new Iterator<>() {

            private SolaceRecord<T> pending;

            private int read;

            @Override
            public boolean hasNext() {
                if (this.pending != null) {
                    return true;
                }
                if (this.read >= limit) {
                    return false;
                }
                // Qualified: an unqualified next() would bind to this Iterator's own next(),
                // not the browser's.
                this.pending = DefaultSolaceBrowser.this.next().orElse(null);
                return this.pending != null;
            }

            @Override
            public SolaceRecord<T> next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                SolaceRecord<T> record = this.pending;
                this.pending = null;
                this.read++;
                return record;
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SolaceRecord<T> record) {
        if (record == null || record.getRawMessage() == null) {
            throw new IllegalArgumentException("Only a record returned by this browser can be removed");
        }
        try {
            this.browser.remove(record.getRawMessage());
            log.info("Removed a message from queue '{}' while browsing", this.queue);
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException(
                    "Unable to remove a browsed message from queue '" + this.queue + "'", ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            this.browser.close();
        }
        catch (RuntimeException ex) {
            log.debug("Error closing the browser for queue '{}'", this.queue, ex);
        }
    }
}
