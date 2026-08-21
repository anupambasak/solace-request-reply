package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;

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

    private volatile XMLMessageProducer sharedProducer;

    public DefaultSolaceSessionFactory(SpringJCSMPFactory springJCSMPFactory) {
        this.springJCSMPFactory = springJCSMPFactory;
    }

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

    @Override
    public JCSMPSession createSession() {
        try {
            JCSMPSession session = this.springJCSMPFactory.createSession();
            session.connect();
            this.ownedSessions.add(session);
            log.info("Connected a Solace JCSMP session");
            return session;
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to create a Solace session", ex);
        }
    }

    @Override
    public XMLMessageProducer getSharedProducer() {
        XMLMessageProducer producer = this.sharedProducer;
        if (producer == null) {
            synchronized (this) {
                producer = this.sharedProducer;
                if (producer == null) {
                    try {
                        producer = getSharedSession().getMessageProducer(new LoggingPublishEventHandler());
                        this.sharedProducer = producer;
                    }
                    catch (JCSMPException ex) {
                        throw new SolaceMessagingException("Unable to create the shared Solace producer", ex);
                    }
                }
            }
        }
        return producer;
    }

    @Override
    public TransactedSession createTransactedSession() {
        try {
            // JCSMP refuses to create additional publisher flows -- which is what a transacted
            // session's producer is -- until the session's default publisher exists ("May not
            // create additional publisher flows until the default publisher has been created").
            // A consume-and-reply service never publishes outside a transaction, so nothing else
            // would ever trigger it; force it here, on the session the transacted session belongs to.
            getSharedProducer();
            return getSharedSession().createTransactedSession();
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to create a Solace transacted session", ex);
        }
    }

    @Override
    public void closeSession(JCSMPSession session) {
        if (session == null || session == this.sharedSession) {
            return;
        }
        this.ownedSessions.remove(session);
        session.closeSession();
    }

    @Override
    public void destroy() {
        XMLMessageProducer producer = this.sharedProducer;
        if (producer != null) {
            producer.close();
            this.sharedProducer = null;
        }
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

        @Override
        public void responseReceivedEx(Object key) {
            if (log.isTraceEnabled()) {
                log.trace("Publish acknowledged for correlation key {}", key);
            }
        }

        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            log.error("Publish failed for correlation key {} at {}", key, timestamp, cause);
        }
    }
}
