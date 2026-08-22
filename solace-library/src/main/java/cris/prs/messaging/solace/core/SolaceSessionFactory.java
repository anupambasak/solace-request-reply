package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

/**
 * Supplies connected JCSMP sessions &mdash; the Solace analogue of Spring for Apache Kafka's
 * {@code ProducerFactory} and {@code ConsumerFactory} combined.
 *
 * <p>The factory instance is also the <em>resource key</em> that Solace transactions bind under in
 * {@code TransactionSynchronizationManager}, so a template and the listener containers that should
 * share its transactions must be given the same instance.</p>
 *
 * <p>Two JCSMP rules shape this contract:</p>
 * <ul>
 *   <li>a session's <em>default publisher</em> must exist before any other publisher flow is created
 *       on it, including a transacted session's producer;</li>
 *   <li>the broker limits <em>transacted sessions per client connection</em> (10 by default), so
 *       components needing many of them have to spread across connections.</li>
 * </ul>
 *
 * @see DefaultSolaceSessionFactory
 */
public interface SolaceSessionFactory {

    /**
     * The shared, connected session.
     *
     * <p>JCSMP sessions are thread safe, so one session backs the template and every
     * non-transactional listener flow unless a component asks for its own. Created and connected on
     * first use.</p>
     *
     * @return the shared session, never {@code null}
     * @throws SolaceMessagingException if the session cannot be created or connected
     */
    JCSMPSession getSharedSession();

    /**
     * Create a new connected session owned by the caller.
     *
     * <p>Used where a component must not share the shared session's limits or subscriptions: a
     * transactional container needs its own transacted-session allowance, and a direct-mode
     * container needs its session-level subscriptions isolated from other containers.</p>
     *
     * @return a newly connected session; release it with {@link #closeSession(JCSMPSession)}
     * @throws SolaceMessagingException if the session cannot be created or connected
     */
    JCSMPSession createSession();

    /**
     * The default publisher of {@link #getSharedSession()}.
     *
     * @return the shared producer, created on first use
     * @throws SolaceMessagingException if the producer cannot be created
     */
    XMLMessageProducer getSharedProducer();

    /**
     * The default publisher of a specific session, created on first use.
     *
     * <p>JCSMP refuses to create additional publisher flows on a session until its default publisher
     * exists, and that rule is per connection rather than per application.</p>
     *
     * @param session the session whose default publisher is wanted
     * @return the producer bound to that session
     * @throws SolaceMessagingException if the producer cannot be created
     */
    XMLMessageProducer getProducer(JCSMPSession session);

    /**
     * Create a transacted session on the shared session, for a Solace local transaction.
     *
     * @return a new transacted session; the caller is responsible for closing it
     * @throws SolaceMessagingException if the transacted session cannot be created, which includes
     *                                  the connection having reached its transacted-session limit
     */
    TransactedSession createTransactedSession();

    /**
     * Create a transacted session on a specific connection.
     *
     * <p>Solace caps transacted sessions per client connection, so a service with several
     * transactional listeners must spread them over more than one connection rather than taking
     * them all from the shared session. Implementations must also ensure the given session's default
     * publisher exists, since a transacted producer is an additional publisher flow.</p>
     *
     * @param session the connection to take the transacted session from
     * @return a new transacted session; the caller is responsible for closing it
     * @throws SolaceMessagingException if the transacted session cannot be created
     */
    TransactedSession createTransactedSession(JCSMPSession session);

    /**
     * Close a session previously handed out by {@link #createSession()}.
     *
     * <p>Implementations ignore the shared session, so a caller need not check which kind it holds.</p>
     *
     * @param session the session to close; {@code null} is ignored
     */
    void closeSession(JCSMPSession session);
}
