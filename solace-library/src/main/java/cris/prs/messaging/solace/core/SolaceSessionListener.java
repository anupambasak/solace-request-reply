package cris.prs.messaging.solace.core;

/**
 * Notified of session lifecycle events on the connection to the broker.
 *
 * <p>The session is the layer below flows: JCSMP reconnects it transparently, so a network blip can
 * interrupt every consumer and producer in the application and then repair itself with no flow event
 * and no other trace. These are the only events that make that visible.</p>
 *
 * <pre>{@code
 * @Bean
 * SolaceSessionListener solaceSessionListener(AlertService alerts) {
 *     return args -> {
 *         switch (args.getEvent()) {
 *             case VIRTUAL_ROUTER_NAME_CHANGED ->
 *                     // Failed over to the other broker in the HA pair: temporary endpoints and
 *                     // unacknowledged guaranteed messages did not come with us.
 *                     cache.invalidateAll();
 *             case DOWN -> alerts.page("Solace session down", args.getException());
 *             default   -> { }
 *         }
 *     };
 * }
 * }</pre>
 *
 * <p>Called on a JCSMP notification thread, so an implementation must be quick and must not block.
 * The session factory guards every call: an exception is logged and swallowed rather than propagating
 * into JCSMP, where it would be dropped anyway.</p>
 */
@FunctionalInterface
public interface SolaceSessionListener {

    /**
     * A session lifecycle event occurred.
     *
     * @param args everything known about the event
     */
    void onSessionEvent(SolaceSessionEventArgs args);
}
