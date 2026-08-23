package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import cris.prs.messaging.solace.core.SettlementOutcome;

/** Strategy for dealing with an exception thrown by a listener. */
@FunctionalInterface
public interface SolaceListenerErrorHandler {

    /**
     * Deal with a listener failure.
     *
     * <p>Called <em>before</em> the message is settled, so an implementation may record the failure,
     * publish a copy elsewhere, or increment a metric while the message is still in hand. What
     * happens to the message afterwards is decided by
     * {@link #resolveOutcome(BytesXMLMessage, Exception)} and the container's configured outcome.</p>
     *
     * @param message   the message whose listener threw
     * @param exception what it threw
     */
    void handleError(BytesXMLMessage message, Exception exception);

    /**
     * Decide what to do with this particular message, overriding the container's configured outcome.
     *
     * <p>This is where a per-failure policy belongs. The container's {@code errorOutcome} is a single
     * answer for every failure; a handler can be more discerning, because the right answer usually
     * depends on <em>why</em> it failed rather than on which listener it was:</p>
     *
     * <pre>{@code
     * @Override
     * public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
     *     if (exception instanceof SolaceMessagingException) {
     *         return SettlementOutcome.REJECTED;   // will never deserialise; do not retry it
     *     }
     *     if (exception instanceof TimeoutException) {
     *         return SettlementOutcome.FAILED;     // transient; hand it back for redelivery
     *     }
     *     return null;                             // anything else: the container decides
     * }
     * }</pre>
     *
     * <p>A handler that may return {@link SettlementOutcome#FAILED} or
     * {@link SettlementOutcome#REJECTED} needs the flow to have negotiated those outcomes at bind
     * time. The container derives that from its configured {@code errorOutcome}, which cannot see
     * what a handler will decide at runtime &mdash; so set
     * {@code solace.listener.negative-acknowledgement: true} explicitly when using this method.</p>
     *
     * <p>Ignored on a transacted flow, where the transaction's rollback governs redelivery.</p>
     *
     * @param message   the message whose listener threw
     * @param exception what it threw
     * @return the outcome to apply, or {@code null} to use the container's configured outcome. The
     *         default returns {@code null}, so an error handler written as a lambda keeps working
     *         unchanged
     */
    default SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
        return null;
    }
}
