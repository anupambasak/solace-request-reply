package cris.prs.messaging.solace.listener;

/**
 * Notified of flow lifecycle events on a listener container.
 *
 * <p>Flow events are the only way an application learns that consumption stopped and restarted, or
 * that this instance became the active consumer on an exclusive endpoint. The container logs every
 * event by default; implement this to act on one.</p>
 *
 * <p>Two things it is good for:</p>
 *
 * <pre>{@code
 * // 1. Leader election, for free, on an exclusive endpoint
 * factory.setFlowListener(args -> {
 *     switch (args.getEvent()) {
 *         case ACTIVE   -> scheduler.becomeLeader();
 *         case INACTIVE -> scheduler.standDown();
 *         default       -> { }
 *     }
 * });
 *
 * // 2. Alerting on a flow that will not come back by itself
 * factory.setFlowListener(args -> {
 *     if (args.getEvent() == SolaceFlowEvent.DOWN) {
 *         alerts.page("Solace flow down on " + args.getEndpoint(), args.getException());
 *     }
 * });
 * }</pre>
 *
 * <p>Called on a JCSMP notification thread, so an implementation must be quick and must not block.
 * The container guards every call: an exception is logged and swallowed rather than propagating into
 * JCSMP, where it would be dropped anyway.</p>
 */
@FunctionalInterface
public interface SolaceFlowListener {

    /**
     * A flow lifecycle event occurred.
     *
     * @param args everything known about the event
     */
    void onFlowEvent(SolaceFlowEventArgs args);
}
