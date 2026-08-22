package cris.prs.messaging.solace.requestreply;

import lombok.Getter;

import java.util.concurrent.CompletableFuture;

/**
 * The future returned by {@link ReplyingSolaceTemplate}, carrying the correlation id and the
 * timings needed to report round-trip latency &mdash; the analogue of Spring for Apache Kafka's
 * {@code RequestReplyFuture}.
 *
 * @param <R> the reply payload type
 */
@Getter
public class RequestReplyFuture<R> extends CompletableFuture<R> {

    private final String correlationId;

    private final long sendTime;

    private final String requestDestination;

    private final String replyDestination;

    private volatile long receiveTime;

    /**
     * @param correlationId      correlation id sent with the request, used to match the reply
     * @param sendTime           millisecond epoch at which the request was published
     * @param requestDestination topic the request was published to
     * @param replyDestination   destination the reply is expected on
     */
    public RequestReplyFuture(String correlationId, long sendTime, String requestDestination,
            String replyDestination) {
        this.correlationId = correlationId;
        this.sendTime = sendTime;
        this.requestDestination = requestDestination;
        this.replyDestination = replyDestination;
    }

    void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }

    /** Round-trip latency in milliseconds, valid once the future has completed with a reply. */
    public long getLatency() {
        return this.receiveTime - this.sendTime;
    }
}
