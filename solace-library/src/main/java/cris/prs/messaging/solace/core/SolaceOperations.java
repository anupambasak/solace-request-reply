package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.XMLMessage;
import org.springframework.messaging.Message;

import java.util.Map;

/**
 * Publishing operations &mdash; the Solace analogue of Spring for Apache Kafka's
 * {@code KafkaOperations}.
 *
 * <p>Every {@code send} joins the Solace transaction bound to the current thread when one is active,
 * so publishing from inside {@code @Transactional} code is transactional with no separate API. When
 * no transaction is active the message is published immediately through the shared producer.</p>
 *
 * <p>Destination names are topics by default. A name prefixed {@code queue:} addresses a queue
 * instead, so {@code "queue:audit"} publishes straight to the endpoint {@code audit}.</p>
 *
 * @param <T> the default payload type
 * @see SolaceTemplate
 */
public interface SolaceOperations<T> {

    /**
     * Publish to the configured default destination.
     *
     * @param payload the payload to send
     * @throws IllegalStateException    if no default destination is configured
     * @throws SolaceMessagingException if the publish fails
     */
    void send(T payload);

    /**
     * Publish to a destination.
     *
     * @param destination topic name, or {@code queue:name} to address a queue
     * @param payload     the payload to send
     * @throws SolaceMessagingException if the publish fails
     */
    void send(String destination, T payload);

    /**
     * Publish with a correlation id, which is written to the message's native correlation id field.
     *
     * @param destination   topic name, or {@code queue:name} to address a queue
     * @param correlationId value for the {@code solace_correlationId} header
     * @param payload       the payload to send
     * @throws SolaceMessagingException if the publish fails
     */
    void send(String destination, String correlationId, T payload);

    /**
     * Publish with additional headers.
     *
     * <p>Headers named after a {@link SolaceHeaders} constant map to native message fields; every
     * other header becomes an SDT user property and stays usable in broker-side selectors.</p>
     *
     * @param destination topic name, or {@code queue:name} to address a queue
     * @param payload     the payload to send
     * @param headers     headers to apply; may be {@code null}
     * @throws SolaceMessagingException if the publish fails
     */
    void send(String destination, T payload, Map<String, Object> headers);

    /**
     * Publish a Spring {@code Message}.
     *
     * <p>The destination comes from the {@link SolaceHeaders#TARGET_DESTINATION} header, falling
     * back to the configured default destination.</p>
     *
     * @param message the message to send; its headers are applied to the Solace message
     * @throws IllegalStateException    if the message carries no target destination and no default
     *                                  is configured
     * @throws SolaceMessagingException if the publish fails
     */
    void send(Message<?> message);

    /**
     * Publish an already built Solace message.
     *
     * <p>The escape hatch for anything the conversion and header mapping do not cover: the message
     * is published exactly as given, with no defaults applied.</p>
     *
     * @param destination the resolved Solace destination
     * @param message     the message to publish
     * @throws SolaceMessagingException if the publish fails
     */
    void send(Destination destination, XMLMessage message);

    /**
     * Run the callback inside a Solace local transaction, committing on normal return and rolling
     * back on exception.
     *
     * <p>Joins an existing transaction rather than nesting one, so calling it inside
     * {@code @Transactional} code is safe and does not create a second transacted session.</p>
     *
     * <p>Messages published in a transaction only reach the broker at commit. A request whose reply
     * is awaited must therefore be waited on after this method returns.</p>
     *
     * @param callback the work to perform
     * @param <R>      the callback's result type
     * @return whatever the callback returned
     * @throws SolaceMessagingException if the commit or rollback fails
     */
    <R> R executeInTransaction(TransactionCallback<T, R> callback);

    /**
     * Read messages from a queue without consuming them.
     *
     * <p>The operator's view of an endpoint: what is on the dead message queue, what a stuck backlog
     * contains, what a poison message actually says. Nothing is acknowledged, so every message stays
     * spooled and is still delivered to whatever consumer is bound.</p>
     *
     * <p>The returned browser holds a bind on the endpoint and <b>must be closed</b>.</p>
     *
     * @param queue       name of the queue to browse
     * @param payloadType the type message bodies are converted into
     * @param <B>         the browsed payload type
     * @return an open browser; close it, ideally with try-with-resources
     */
    <B> SolaceBrowser<B> browse(String queue, Class<B> payloadType);

    /**
     * Read messages from a queue without consuming them, with a selector or a wait timeout.
     *
     * @param spec        what to browse and how
     * @param payloadType the type message bodies are converted into
     * @param <B>         the browsed payload type
     * @return an open browser; close it, ideally with try-with-resources
     */
    <B> SolaceBrowser<B> browse(BrowseSpec spec, Class<B> payloadType);

    /**
     * Callback for {@link #executeInTransaction(TransactionCallback)}.
     *
     * @param <T> the operations' payload type
     * @param <R> the result type
     */
    @FunctionalInterface
    interface TransactionCallback<T, R> {

        /**
         * Perform the transactional work.
         *
         * @param operations the same operations instance, whose sends now join the transaction
         * @return the result to hand back to the caller; may be {@code null}
         */
        R doInSolace(SolaceOperations<T> operations);
    }
}
