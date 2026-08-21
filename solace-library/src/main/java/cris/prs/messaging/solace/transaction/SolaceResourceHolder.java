package cris.prs.messaging.solace.transaction;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import cris.prs.messaging.solace.core.DefaultSolaceSessionFactory;
import cris.prs.messaging.solace.core.SolaceMessagingException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * Holds the {@code TransactedSession} bound to the current thread for the duration of a Solace
 * local transaction, together with the producer created from it. Publishes made through that
 * producer only reach consumers when the transaction commits.
 */
@Slf4j
@Getter
public class SolaceResourceHolder extends ResourceHolderSupport {

    private final TransactedSession transactedSession;

    /**
     * {@code true} when the transacted session belongs to a listener container rather than to the
     * transaction manager; such sessions are committed but never closed by the manager.
     */
    private final boolean externallyManaged;

    private XMLMessageProducer producer;

    public SolaceResourceHolder(TransactedSession transactedSession) {
        this(transactedSession, false);
    }

    public SolaceResourceHolder(TransactedSession transactedSession, boolean externallyManaged) {
        this.transactedSession = transactedSession;
        this.externallyManaged = externallyManaged;
    }

    /** The producer bound to this transaction, created lazily on first publish. */
    public XMLMessageProducer getProducer() {
        if (this.producer == null) {
            try {
                this.producer = this.transactedSession.createProducer(new ProducerFlowProperties(),
                        new DefaultSolaceSessionFactory.LoggingPublishEventHandler());
            }
            catch (JCSMPException ex) {
                throw new SolaceMessagingException("Unable to create a transacted Solace producer", ex);
            }
        }
        return this.producer;
    }

    public void commit() {
        try {
            this.transactedSession.commit();
        }
        catch (RollbackException ex) {
            throw new SolaceMessagingException("Solace transaction was rolled back by the broker", ex);
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to commit the Solace transaction", ex);
        }
    }

    public void rollback() {
        try {
            this.transactedSession.rollback();
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to roll back the Solace transaction", ex);
        }
    }

    /** Release the transacted session unless a listener container owns it. */
    public void closeIfOwned() {
        if (this.externallyManaged) {
            this.producer = null;
            return;
        }
        try {
            if (this.producer != null) {
                this.producer.close();
            }
            this.transactedSession.close();
        }
        catch (Exception ex) {
            log.debug("Error closing transacted Solace session", ex);
        }
        finally {
            this.producer = null;
        }
    }
}
