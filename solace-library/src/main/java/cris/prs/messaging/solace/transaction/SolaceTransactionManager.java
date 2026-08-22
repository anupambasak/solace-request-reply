package cris.prs.messaging.solace.transaction;

import cris.prs.messaging.solace.core.SolaceSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

/**
 * {@code PlatformTransactionManager} backed by a Solace {@code TransactedSession}, so that Solace
 * local transactions can be driven by {@code @Transactional} or a {@code TransactionTemplate} on
 * either side of a request-reply exchange.
 *
 * <p>Within a transaction, every publish made through a {@code SolaceTemplate} and every message
 * consumed by a transactional listener container is staged on the same transacted session: the
 * commit both acknowledges the consumed messages and releases the published ones, as described in
 * the Solace <em>Transactions</em> documentation.</p>
 *
 * <p>When a transactional listener container is active it binds its own transacted session before
 * starting the transaction, which is what makes "consume the request and publish the reply
 * atomically" work. In all other cases this manager creates and closes the session itself.</p>
 */
@Slf4j
public class SolaceTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

    private final SolaceSessionFactory sessionFactory;

    /**
     * @param sessionFactory supplies transacted sessions, and is the key transactional resources are
     *                       bound under &mdash; templates and containers sharing transactions must
     *                       share this instance
     */
    public SolaceTransactionManager(SolaceSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        setNestedTransactionAllowed(false);
        setTransactionSynchronization(SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
    }

    @Override
    /**
     * @return the session factory, so that {@code TransactionSynchronizationManager} keys resources
     *         the same way {@code SolaceTemplate} looks them up
     */
    public Object getResourceFactory() {
        return this.sessionFactory;
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>Picks up a holder already bound to the thread, which is how a listener container's own
     * transacted session becomes this transaction's resource.</p>
     */
    protected Object doGetTransaction() {
        SolaceTransactionObject txObject = new SolaceTransactionObject();
        txObject.setResourceHolder(SolaceTransactionUtils.getResourceHolder(this.sessionFactory));
        return txObject;
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>A holder counts as an existing transaction only once it has been marked synchronized, so a
     * container-bound holder still causes a fresh transaction to begin.</p>
     */
    protected boolean isExistingTransaction(Object transaction) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) transaction;
        return txObject.getResourceHolder() != null
                && txObject.getResourceHolder().isSynchronizedWithTransaction();
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>Creates and binds a transacted session when none is bound. Solace has no explicit "begin":
     * a transacted session is always in a transaction, so this only establishes the resource.</p>
     */
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) transaction;
        try {
            if (txObject.getResourceHolder() == null) {
                SolaceResourceHolder holder = new SolaceResourceHolder(this.sessionFactory.createTransactedSession());
                txObject.setResourceHolder(holder);
                txObject.setNewResourceHolder(true);
                SolaceTransactionUtils.bindResourceHolder(this.sessionFactory, holder);
            }
            SolaceResourceHolder holder = txObject.getResourceHolder();
            holder.setSynchronizedWithTransaction(true);
            int timeout = determineTimeout(definition);
            if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
                holder.setTimeoutInSeconds(timeout);
            }
        }
        catch (RuntimeException ex) {
            if (txObject.isNewResourceHolder()) {
                SolaceTransactionUtils.unbindResourceHolder(this.sessionFactory);
                txObject.getResourceHolder().closeIfOwned();
                txObject.setResourceHolder(null);
                txObject.setNewResourceHolder(false);
            }
            throw new CannotCreateTransactionException("Unable to begin a Solace transaction", ex);
        }
    }

    @Override
    /** {@inheritDoc} */
    protected void doCommit(DefaultTransactionStatus status) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) status.getTransaction();
        if (log.isTraceEnabled()) {
            log.trace("Committing Solace transaction on session {}",
                    txObject.getResourceHolder().getTransactedSession().getName());
        }
        txObject.getResourceHolder().commit();
    }

    @Override
    /** {@inheritDoc} */
    protected void doRollback(DefaultTransactionStatus status) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) status.getTransaction();
        log.debug("Rolling back Solace transaction");
        txObject.getResourceHolder().rollback();
    }

    @Override
    /** {@inheritDoc} */
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) status.getTransaction();
        txObject.getResourceHolder().setRollbackOnly();
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>Unbinds and closes only a session this manager created; a container's session is left open
     * for the next message.</p>
     */
    protected void doCleanupAfterCompletion(Object transaction) {
        SolaceTransactionObject txObject = (SolaceTransactionObject) transaction;
        SolaceResourceHolder holder = txObject.getResourceHolder();
        if (holder == null) {
            return;
        }
        if (txObject.isNewResourceHolder()) {
            SolaceTransactionUtils.unbindResourceHolder(this.sessionFactory);
            holder.closeIfOwned();
        }
        holder.clear();
    }

    /** Transaction object carrying the thread-bound {@link SolaceResourceHolder}. */
    private static class SolaceTransactionObject implements SmartTransactionObject {

        private SolaceResourceHolder resourceHolder;

        private boolean newResourceHolder;

        SolaceResourceHolder getResourceHolder() {
            return this.resourceHolder;
        }

        void setResourceHolder(SolaceResourceHolder resourceHolder) {
            this.resourceHolder = resourceHolder;
        }

        boolean isNewResourceHolder() {
            return this.newResourceHolder;
        }

        void setNewResourceHolder(boolean newResourceHolder) {
            this.newResourceHolder = newResourceHolder;
        }

        @Override
        public boolean isRollbackOnly() {
            return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
        }

        @Override
        public void flush() {
            TransactionSynchronizationUtils.triggerFlush();
        }
    }
}
