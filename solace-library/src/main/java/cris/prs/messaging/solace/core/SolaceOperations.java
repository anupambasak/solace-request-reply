package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.XMLMessage;
import org.springframework.messaging.Message;

import java.util.Map;

/**
 * Publishing operations &mdash; the Solace analogue of Spring for Apache Kafka's
 * {@code KafkaOperations}.
 *
 * <p>Every {@code send} joins the Solace transaction bound to the current thread when one is
 * active, so publishing from inside {@code @Transactional} code is transactional without any
 * further configuration.</p>
 *
 * @param <T> the default payload type
 */
public interface SolaceOperations<T> {

    /** Publish to the configured default destination. */
    void send(T payload);

    void send(String destination, T payload);

    void send(String destination, String correlationId, T payload);

    void send(String destination, T payload, Map<String, Object> headers);

    /** Publish a Spring {@code Message}; the destination comes from the target-destination header. */
    void send(Message<?> message);

    /** Publish an already built Solace message. */
    void send(Destination destination, XMLMessage message);

    /**
     * Run the callback inside a Solace local transaction, committing on normal return and rolling
     * back on exception.
     */
    <R> R executeInTransaction(TransactionCallback<T, R> callback);

    /** Callback for {@link #executeInTransaction(TransactionCallback)}. */
    @FunctionalInterface
    interface TransactionCallback<T, R> {

        R doInSolace(SolaceOperations<T> operations);
    }
}
