package cris.prs.messaging.solace.transaction;

import cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Thread-bound lookup of the Solace transactional resources, mirroring Spring's JMS utilities. */
public final class SolaceTransactionUtils {

    private SolaceTransactionUtils() {
    }

    /** The resource holder bound to the current thread, or {@code null} when no transaction is active. */
    @Nullable
    public static SolaceResourceHolder getResourceHolder(SolaceSessionFactory sessionFactory) {
        return (SolaceResourceHolder) TransactionSynchronizationManager.getResource(sessionFactory);
    }

    /**
     * The resource holder of an <em>active</em> transaction &mdash; one that a transaction manager
     * has actually begun. Producers must only join such a transaction.
     */
    @Nullable
    public static SolaceResourceHolder getActiveResourceHolder(SolaceSessionFactory sessionFactory) {
        SolaceResourceHolder holder = getResourceHolder(sessionFactory);
        return (holder != null && holder.isSynchronizedWithTransaction()) ? holder : null;
    }

    public static void bindResourceHolder(SolaceSessionFactory sessionFactory, SolaceResourceHolder holder) {
        TransactionSynchronizationManager.bindResource(sessionFactory, holder);
    }

    public static void unbindResourceHolder(SolaceSessionFactory sessionFactory) {
        if (TransactionSynchronizationManager.hasResource(sessionFactory)) {
            TransactionSynchronizationManager.unbindResource(sessionFactory);
        }
    }
}
