package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

/**
 * Supplies connected {@code JCSMPSession}s &mdash; the Solace analogue of Spring for Apache Kafka's
 * {@code ProducerFactory}/{@code ConsumerFactory}.
 */
public interface SolaceSessionFactory {

    /**
     * A shared, connected session. JCSMP sessions are thread safe, so a single session backs the
     * template and all non-transactional listener flows unless a component asks for its own.
     */
    JCSMPSession getSharedSession();

    /** A new, connected session owned by the caller. */
    JCSMPSession createSession();

    /** The shared producer bound to {@link #getSharedSession()}. */
    XMLMessageProducer getSharedProducer();

    /** A new transacted session for local transactions. */
    TransactedSession createTransactedSession();

    /** Close a session previously handed out by {@link #createSession()}. */
    void closeSession(JCSMPSession session);
}
