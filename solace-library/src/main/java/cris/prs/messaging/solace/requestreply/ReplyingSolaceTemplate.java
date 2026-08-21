package cris.prs.messaging.solace.requestreply;

import com.solacesystems.jcsmp.BytesXMLMessage;
import cris.prs.messaging.solace.core.SolaceHeaders;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.core.SolaceTemplate;
import cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous request-reply over Solace &mdash; the counterpart of {@code ReplyingKafkaTemplate}.

 * <p>Each request is stamped with a correlation id and a {@code replyTo} destination that is unique
 * to this application instance: the reply topic carries the pod/host name as its last level. A
 * horizontally scaled deployment therefore needs neither broker side selectors nor a shared reply
 * queue &mdash; a reply can only reach the instance that asked for it.</p>
 *
 * <p>The returned {@link RequestReplyFuture} completes when the matching reply arrives, or fails
 * with {@link SolaceReplyTimeoutException} once the reply timeout elapses.</p>
 *
 * <p><b>Transactions:</b> when a Solace transaction is active on the calling thread the request is
 * only released to the broker at commit, so the future must be waited on <em>outside</em> the
 * transaction.</p>
 */
@Slf4j
public class ReplyingSolaceTemplate extends SolaceTemplate<Object>
        implements SmartLifecycle, InitializingBean, DisposableBean {

    private final SolaceMessageListenerContainer replyContainer;

    private final Map<String, PendingRequest> pending = new ConcurrentHashMap<>();

    /** The instance-specific destination that requests ask replies to be sent to. */
    @Getter
    private final String replyDestination;

    @Setter
    private Duration defaultReplyTimeout = Duration.ofSeconds(30);

    /** Identifier of this instance, carried on every request as an SDT user property. */
    @Setter
    private String instanceId;

    @Setter
    private boolean autoStartup = true;

    @Setter
    private int phase = Integer.MAX_VALUE - 90;

    private volatile boolean running;

    private ScheduledExecutorService timeoutScheduler;

    public ReplyingSolaceTemplate(SolaceSessionFactory sessionFactory, SolaceMessageConverter messageConverter,
            SolaceMessageListenerContainer replyContainer, String replyDestination) {
        super(sessionFactory, messageConverter);
        Assert.notNull(replyContainer, "'replyContainer' must not be null");
        Assert.hasText(replyDestination, "'replyDestination' must not be empty");
        this.replyContainer = replyContainer;
        this.replyDestination = replyDestination;
    }

    @Override
    public void afterPropertiesSet() {
        this.replyContainer.setupMessageListener(this::onReply);
    }

    // --- request/reply -------------------------------------------------------------------

    public <T> RequestReplyFuture<T> sendAndReceive(String destination, Object payload, Class<T> replyType) {
        return sendAndReceive(destination, payload, null, replyType, this.defaultReplyTimeout);
    }

    public <T> RequestReplyFuture<T> sendAndReceive(String destination, Object payload, Class<T> replyType,
            Duration replyTimeout) {
        return sendAndReceive(destination, payload, null, replyType, replyTimeout);
    }

    /**
     * Publish a request and return a future for its reply.
     *
     * @param destination  the request topic
     * @param payload      the request payload
     * @param headers      extra headers, carried as SDT user properties (may be {@code null})
     * @param replyType    the type the reply body is converted into
     * @param replyTimeout how long to wait before failing the future
     */
    public <T> RequestReplyFuture<T> sendAndReceive(String destination, Object payload, Map<String, Object> headers,
            Class<T> replyType, Duration replyTimeout) {
        Assert.state(this.running, "ReplyingSolaceTemplate is not running");
        String correlationId = UUID.randomUUID().toString();
        long sendTime = System.currentTimeMillis();
        RequestReplyFuture<T> future =
                new RequestReplyFuture<>(correlationId, sendTime, destination, this.replyDestination);

        Map<String, Object> requestHeaders = new HashMap<>();
        if (headers != null) {
            requestHeaders.putAll(headers);
        }
        requestHeaders.put(SolaceHeaders.CORRELATION_ID, correlationId);
        requestHeaders.put(SolaceHeaders.REPLY_TO, this.replyDestination);
        requestHeaders.put(SolaceHeaders.REQUEST_SEND_TIME, sendTime);
        if (this.instanceId != null) {
            requestHeaders.put(SolaceHeaders.INSTANCE_ID, this.instanceId);
        }

        PendingRequest pendingRequest = new PendingRequest(future, replyType);
        this.pending.put(correlationId, pendingRequest);
        try {
            send(destination, payload, requestHeaders);
        }
        catch (RuntimeException ex) {
            this.pending.remove(correlationId);
            future.completeExceptionally(ex);
            return future;
        }
        scheduleTimeout(correlationId, future,
                replyTimeout != null ? replyTimeout : this.defaultReplyTimeout);
        if (log.isTraceEnabled()) {
            log.trace("Sent request correlationId={} to {} expecting a reply on {}", correlationId,
                    destination, this.replyDestination);
        }
        return future;
    }

    private void scheduleTimeout(String correlationId, RequestReplyFuture<?> future, Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return;
        }
        ScheduledFuture<?> scheduled = this.timeoutScheduler.schedule(() -> {
            if (this.pending.remove(correlationId) != null) {
                future.completeExceptionally(new SolaceReplyTimeoutException(
                        "No reply received for correlationId=" + correlationId + " within " + timeout));
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
        future.whenComplete((reply, error) -> scheduled.cancel(false));
    }

    /** Reply listener: matches the correlation id and completes the waiting future. */
    @SuppressWarnings("unchecked")
    protected void onReply(BytesXMLMessage message) {
        String correlationId = message.getCorrelationId();
        PendingRequest pendingRequest = correlationId != null ? this.pending.remove(correlationId) : null;
        if (pendingRequest == null) {
            log.warn("Received a reply with no outstanding request, correlationId={}", correlationId);
            return;
        }
        RequestReplyFuture<Object> future = (RequestReplyFuture<Object>) pendingRequest.future();
        try {
            Object reply = getMessageConverter().fromMessage(message, pendingRequest.replyType());
            future.setReceiveTime(System.currentTimeMillis());
            future.complete(reply);
        }
        catch (RuntimeException ex) {
            future.completeExceptionally(ex);
        }
    }

    /** Number of requests still awaiting a reply; useful for metrics and tests. */
    public int getPendingCount() {
        return this.pending.size();
    }

    // --- lifecycle -----------------------------------------------------------------------

    @Override
    public void start() {
        if (this.running) {
            return;
        }
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "solace-reply-timeout");
            thread.setDaemon(true);
            return thread;
        });
        this.replyContainer.start();
        this.running = true;
        log.info("ReplyingSolaceTemplate started, replies expected on '{}'", this.replyDestination);
    }

    @Override
    public void stop() {
        if (!this.running) {
            return;
        }
        this.running = false;
        this.replyContainer.stop();
        if (this.timeoutScheduler != null) {
            this.timeoutScheduler.shutdownNow();
        }
        this.pending.values().forEach(pendingRequest -> pendingRequest.future()
                .completeExceptionally(new SolaceReplyTimeoutException(
                        "The ReplyingSolaceTemplate was stopped before the reply arrived")));
        this.pending.clear();
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    @Override
    public int getPhase() {
        return this.phase;
    }

    @Override
    public void destroy() {
        stop();
    }

    private record PendingRequest(RequestReplyFuture<?> future, Class<?> replyType) {
    }
}
