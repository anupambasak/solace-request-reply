package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.statistics.StatType;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link SolaceSessionFactory}, built on the {@code SpringJCSMPFactory} contributed by the
 * {@code solace-java-spring-boot-starter} (configured through {@code solace.java.*}).
 */
@Slf4j
public class DefaultSolaceSessionFactory implements SolaceSessionFactory, DisposableBean {

    private final SpringJCSMPFactory springJCSMPFactory;

    private final Set<JCSMPSession> ownedSessions = ConcurrentHashMap.newKeySet();

    private volatile JCSMPSession sharedSession;

    /** The default publisher of each session, which JCSMP requires before any other publisher flow. */
    private final Map<JCSMPSession, XMLMessageProducer> producers = new ConcurrentHashMap<>();

    /**
     * Notified of session lifecycle events; {@code null} means only this factory's own logging.
     */
    @Setter
    private SolaceSessionListener sessionListener;

    /** The connection state, updated from JCSMP session events. */
    private volatile SolaceSessionState sessionState = SolaceSessionState.NOT_CONNECTED;

    /**
     * Create a session factory.
     *
     * @param springJCSMPFactory the factory contributed by {@code solace-java-spring-boot-starter},
     *                           carrying the connection settings bound from {@code solace.java.*}
     */
    public DefaultSolaceSessionFactory(SpringJCSMPFactory springJCSMPFactory) {
        this.springJCSMPFactory = springJCSMPFactory;
    }

    /** {@inheritDoc} */
    @Override
    public JCSMPSession getSharedSession() {
        JCSMPSession session = this.sharedSession;
        if (session == null) {
            synchronized (this) {
                session = this.sharedSession;
                if (session == null) {
                    session = createSession();
                    this.sharedSession = session;
                }
            }
        }
        return session;
    }

    /** {@inheritDoc} */
    @Override
    public JCSMPSession createSession() {
        try {
            // createSession() is exactly createSession(null, null); passing a handler is the only
            // way to observe a reconnect, which JCSMP otherwise repairs entirely silently.
            JCSMPSession session = this.springJCSMPFactory.createSession(null, this::handleSessionEvent);
            session.connect();
            this.ownedSessions.add(session);
            this.sessionState = SolaceSessionState.CONNECTED;
            log.info("Connected a Solace JCSMP session");
            return session;
        }
        catch (JCSMPException ex) {
            this.sessionState = SolaceSessionState.DOWN;
            throw new SolaceMessagingException("Unable to create a Solace session", ex);
        }
    }

    /**
     * Record and report one session event.
     *
     * <p>Runs on a JCSMP notification thread. Log levels follow what an operator needs to act on:
     * {@code DOWN} is an error because nothing will recover without a restart, {@code RECONNECTING}
     * and {@code VIRTUAL_ROUTER_NAME_CHANGED} are warnings because traffic has stopped or the broker
     * underneath has changed, and the rest are informational.</p>
     *
     * @param args the JCSMP event
     */
    private void handleSessionEvent(SessionEventArgs args) {
        SolaceSessionEvent event = SolaceSessionEvent.from(args.getEvent());

        switch (event) {
            case RECONNECTING -> this.sessionState = SolaceSessionState.RECONNECTING;
            case RECONNECTED -> this.sessionState = SolaceSessionState.CONNECTED;
            case DOWN -> this.sessionState = SolaceSessionState.DOWN;
            default -> { }
        }

        switch (event) {
            case DOWN -> log.error("The Solace session is DOWN and JCSMP has stopped retrying; "
                    + "nothing will be sent or received until the application restarts. {}",
                    args.getInfo(), args.getException());
            case RECONNECTING -> log.warn("The Solace session is reconnecting; nothing is being sent "
                    + "or received. {}", args.getInfo());
            case RECONNECTED -> log.info("The Solace session reconnected");
            case VIRTUAL_ROUTER_NAME_CHANGED -> log.warn("The Solace session reconnected to a "
                    + "different broker. Temporary endpoints and unacknowledged guaranteed messages "
                    + "did not survive the failover. {}", args.getInfo());
            case SUBSCRIPTION_ERROR -> log.error("The broker rejected a session subscription: {}",
                    args.getInfo(), args.getException());
            default -> log.info("Solace session event {}: {}", event, args.getInfo());
        }

        SolaceSessionListener listener = this.sessionListener;
        if (listener == null) {
            return;
        }
        try {
            listener.onSessionEvent(new SolaceSessionEventArgs(event, this.sessionState,
                    args.getInfo(), args.getException(), args.getResponseCode()));
        }
        catch (RuntimeException ex) {
            log.warn("Session listener threw on a {} event", event, ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SolaceSessionState getSessionState() {
        JCSMPSession session = this.sharedSession;
        if (session == null) {
            return this.sessionState == SolaceSessionState.DOWN
                    ? SolaceSessionState.DOWN
                    : SolaceSessionState.NOT_CONNECTED;
        }
        return session.isClosed() ? SolaceSessionState.DOWN : this.sessionState;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Reads the shared session's counters, and deliberately does not create one: sampling
     * statistics must never be the thing that opens a connection.</p>
     */
    @Override
    public Map<String, Long> getSessionStatistics(Collection<String> statistics) {
        JCSMPSession session = this.sharedSession;
        if (session == null || statistics == null || statistics.isEmpty()) {
            return Map.of();
        }
        Map<String, Long> sampled = new LinkedHashMap<>();
        JCSMPSessionStats stats = session.getSessionStats();
        for (String name : statistics) {
            try {
                StatType statType = StatType.fromString(name);
                if (statType != null) {
                    sampled.put(name, stats.getStat(statType));
                }
                else {
                    log.warn("Unknown Solace session statistic '{}'; skipping it", name);
                }
            }
            catch (Exception ex) {
                log.debug("Unable to read the Solace session statistic '{}'", name, ex);
            }
        }
        return sampled;
    }

    /** {@inheritDoc} */
    @Override
    public XMLMessageProducer getSharedProducer() {
        return getProducer(getSharedSession());
    }

    /**
     * {@inheritDoc}
     *
     * <p>One producer is cached per session, because the default-publisher rule is per connection.</p>
     */
    @Override
    public XMLMessageProducer getProducer(JCSMPSession session) {
        return this.producers.computeIfAbsent(session, key -> {
            try {
                return key.getMessageProducer(new LoggingPublishEventHandler());
            }
            catch (JCSMPException ex) {
                throw new SolaceMessagingException("Unable to create the Solace default publisher", ex);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public TransactedSession createTransactedSession() {
        return createTransactedSession(getSharedSession());
    }

    /** {@inheritDoc} */
    @Override
    public TransactedSession createTransactedSession(JCSMPSession session) {
        try {
            // JCSMP refuses to create additional publisher flows -- which is what a transacted
            // session's producer is -- until this session's default publisher exists ("May not
            // create additional publisher flows until the default publisher has been created").
            // A consume-and-reply service never publishes outside a transaction, so nothing else
            // would ever trigger it.
            getProducer(session);
            return session.createTransactedSession();
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to create a Solace transacted session. Solace "
                    + "limits transacted sessions per client connection (10 by default), so this can "
                    + "mean the connection is full rather than misconfigured.", ex);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>The shared session is never closed here; it is released by {@link #destroy()}.</p>
     */
    @Override
    public void closeSession(JCSMPSession session) {
        if (session == null || session == this.sharedSession) {
            return;
        }
        this.ownedSessions.remove(session);
        session.closeSession();
    }

    /**
     * Close every producer and every session this factory created.
     *
     * <p>Called by Spring when the application context shuts down.</p>
     */
    @Override
    public void destroy() {
        this.sessionState = SolaceSessionState.DOWN;
        this.producers.values().forEach(producer -> {
            try {
                producer.close();
            }
            catch (Exception ex) {
                log.debug("Error closing a Solace producer", ex);
            }
        });
        this.producers.clear();
        this.ownedSessions.forEach(session -> {
            try {
                session.closeSession();
            }
            catch (Exception ex) {
                log.debug("Error closing Solace session", ex);
            }
        });
        this.ownedSessions.clear();
        this.sharedSession = null;
    }

    /** Publish callback that surfaces asynchronous publish failures in the log. */
    public static class LoggingPublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

        /** Create a publish event handler that logs failures. */
        public LoggingPublishEventHandler() {
        }


        /**
         * Record that the broker accepted a published message.
         *
         * @param key the correlation key of the message, or {@code null} when none was set
         */
        @Override
        public void responseReceivedEx(Object key) {
            if (log.isTraceEnabled()) {
                log.trace("Publish acknowledged for correlation key {}", key);
            }
        }

        /**
         * Report a publish the broker rejected.
         *
         * @param key       the correlation key of the failed message, or {@code null}
         * @param cause     why the broker rejected it
         * @param timestamp when the failure was reported
         */
        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            log.error("Publish failed for correlation key {} at {}", key, timestamp, cause);
        }
    }
}
