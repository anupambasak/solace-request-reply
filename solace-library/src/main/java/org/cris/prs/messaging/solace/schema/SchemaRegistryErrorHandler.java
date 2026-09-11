package org.cris.prs.messaging.solace.schema;

import com.solacesystems.jcsmp.BytesXMLMessage;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.core.SettlementOutcome;
import org.cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.springframework.util.Assert;

/**
 * An error handler that rejects schema failures a retry cannot fix straight to the dead message
 * queue, and leaves every other failure to a delegate.
 *
 * <p>A payload that does not match its schema, names a schema the registry does not have, or arrives
 * without a schema id on a governed destination will fail identically on every redelivery. Returning
 * {@link SettlementOutcome#REJECTED} for those saves the redelivery attempts and unblocks the flow.
 * A registry outage, and anything unrecognised, defers to the delegate and so to the container's
 * configured {@code errorOutcome}.</p>
 *
 * <p>Per-message outcomes must be negotiated when the flow binds, so this handler needs
 * {@code solace.listener.negative-acknowledgement: true}; without it the broker refuses the
 * {@code REJECTED} settlement and the message is redelivered. Transacted containers ignore outcomes
 * entirely &mdash; there the rollback redelivers until {@code max-redelivery-count}.</p>
 */
@Slf4j
public class SchemaRegistryErrorHandler implements SolaceListenerErrorHandler {

    private final SolaceListenerErrorHandler delegate;

    /** Create a handler whose delegate logs the failure, as a container does by default. */
    public SchemaRegistryErrorHandler() {
        this((message, exception) -> log.error("Listener failed for message on {}",
                message.getDestination() != null ? message.getDestination().getName() : "unknown", exception));
    }

    /**
     * Create a handler around an application's own.
     *
     * @param delegate handles every failure, and decides the outcome of everything this handler does
     *                 not reject
     */
    public SchemaRegistryErrorHandler(SolaceListenerErrorHandler delegate) {
        Assert.notNull(delegate, "'delegate' must not be null");
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override
    public void handleError(BytesXMLMessage message, Exception exception) {
        this.delegate.handleError(message, exception);
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link SettlementOutcome#REJECTED} for a non-retryable
     * {@link SchemaRegistryConversionException} anywhere in the cause chain; otherwise whatever the
     * delegate decides.</p>
     */
    @Override
    public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
        SchemaRegistryConversionException failure = find(exception);
        if (failure != null && !failure.getReason().isRetryable()) {
            log.warn("Rejecting message on {} to the dead message queue: {}",
                    message.getDestination() != null ? message.getDestination().getName() : "unknown",
                    failure.getReason());
            return SettlementOutcome.REJECTED;
        }
        return this.delegate.resolveOutcome(message, exception);
    }

    /**
     * Find a schema conversion failure in a cause chain.
     *
     * @param exception what a listener threw; may be {@code null}
     * @return the first {@link SchemaRegistryConversionException} in the chain, or {@code null}
     */
    public static SchemaRegistryConversionException find(Throwable exception) {
        Throwable current = exception;
        int depth = 0;
        while (current != null && depth++ < 20) {
            if (current instanceof SchemaRegistryConversionException failure) {
                return failure;
            }
            if (current.getCause() == current) {
                return null;
            }
            current = current.getCause();
        }
        return null;
    }
}
