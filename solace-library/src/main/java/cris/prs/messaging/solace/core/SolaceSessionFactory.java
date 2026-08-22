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

    /** The default publisher of the given session, created on first use. */
    XMLMessageProducer getProducer(JCSMPSession session);

    /** A new transacted session for local transactions, on the shared session. */
    TransactedSession createTransactedSession();

    /**
     * A new transacted session on a specific connection.
     *
     * <p>Solace caps transacted sessions per client connection (10 by default), so a service with
     * several transactional listeners has to spread them over more than one connection rather than
     * taking them all from the shared session.</p>
     */
    TransactedSession createTransactedSession(JCSMPSession session);

    /** Close a session previously handed out by {@link #createSession()}. */
    void closeSession(JCSMPSession session);
}
