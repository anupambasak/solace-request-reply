package org.cris.prs.messaging.solace.transaction;

import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Thread-bound lookup of the Solace transactional resources, mirroring Spring's JMS utilities. */
public final class SolaceTransactionUtils {

    private SolaceTransactionUtils() {
    }

    /** The resource holder bound to the current thread, or {@code null} when no transaction is active. */
    /**
     * The Solace resources bound to the current thread, whether or not a transaction has begun.
     *
     * @param sessionFactory the resource key
     * @return the holder bound to the current thread, or {@code null} when none is
     */
    @Nullable
    public static SolaceResourceHolder getResourceHolder(SolaceSessionFactory sessionFactory) {
        return (SolaceResourceHolder) TransactionSynchronizationManager.getResource(sessionFactory);
    }

    /**
     * The resource holder of an <em>active</em> transaction &mdash; one that a transaction manager
     * has actually begun. Producers must only join such a transaction.
     */
    /**
     * The holder of an <em>active</em> transaction &mdash; one a transaction manager has actually
     * begun.
     *
     * <p>Producers must join only such a transaction. A container binds its holder before beginning
     * the transaction, so a bound-but-not-yet-begun holder must not capture publishes.</p>
     *
     * @param sessionFactory the resource key
     * @return the active holder, or {@code null}
     */
    @Nullable
    public static SolaceResourceHolder getActiveResourceHolder(SolaceSessionFactory sessionFactory) {
        SolaceResourceHolder holder = getResourceHolder(sessionFactory);
        return (holder != null && holder.isSynchronizedWithTransaction()) ? holder : null;
    }

    /**
     * Bind a holder to the current thread.
     *
     * @param sessionFactory the resource key
     * @param holder         the holder to bind
     */
    public static void bindResourceHolder(SolaceSessionFactory sessionFactory, SolaceResourceHolder holder) {
        TransactionSynchronizationManager.bindResource(sessionFactory, holder);
    }

    /**
     * Unbind the current thread's holder if one is bound.
     *
     * @param sessionFactory the resource key
     */
    public static void unbindResourceHolder(SolaceSessionFactory sessionFactory) {
        if (TransactionSynchronizationManager.hasResource(sessionFactory)) {
            TransactionSynchronizationManager.unbindResource(sessionFactory);
        }
    }
}
