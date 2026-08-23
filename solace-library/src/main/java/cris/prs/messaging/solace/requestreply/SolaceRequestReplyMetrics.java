package cris.prs.messaging.solace.requestreply;

/**
 * Callbacks {@link ReplyingSolaceTemplate} invokes so that request-reply traffic can be measured.
 *
 * <p>Deliberately free of any metrics-library types, so the {@code requestreply} package stays
 * dependency-free and instrumentation is entirely optional. The Micrometer implementation lives in
 * {@code cris.prs.messaging.solace.observability}; when no implementation is supplied a template uses
 * {@link #NO_OP} and records nothing.</p>
 *
 * <p>Implementations must be cheap and must not throw. The template guards every call, so an
 * exception is logged and swallowed rather than failing the request.</p>
 */
public interface SolaceRequestReplyMetrics {

    /** An implementation that records nothing. The default for every template. */
    SolaceRequestReplyMetrics NO_OP = new SolaceRequestReplyMetrics() {
    };

    /**
     * A request has been published.
     *
     * @param templateId  the template's id
     * @param destination the request destination
     */
    default void recordRequest(String templateId, String destination) {
    }

    /**
     * A reply arrived and completed its future.
     *
     * @param templateId    the template's id
     * @param destination   the destination the request went to
     * @param latencyMillis round-trip time measured by the requester
     */
    default void recordReply(String templateId, String destination, long latencyMillis) {
    }

    /**
     * No reply arrived within the request's timeout, and its future was failed.
     *
     * @param templateId  the template's id
     * @param destination the destination the request went to
     */
    default void recordTimeout(String templateId, String destination) {
    }

    /**
     * A reply arrived for which no request was outstanding.
     *
     * <p>Usually a reply that overtook its own timeout. A steady non-zero rate at low volume instead
     * suggests two instances sharing one reply destination.</p>
     *
     * @param templateId the template's id
     */
    default void recordUnmatchedReply(String templateId) {
    }

    /**
     * A request could not be published, and its future was failed.
     *
     * @param templateId  the template's id
     * @param destination the destination the request was addressed to
     */
    default void recordSendFailure(String templateId, String destination) {
    }
}
