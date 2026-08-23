package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.core.SettlementOutcome;
import cris.prs.messaging.solace.core.SolaceFlowEvent;
import cris.prs.messaging.solace.core.SolaceMessagingException;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.transaction.SolaceResourceHolder;
import cris.prs.messaging.solace.transaction.SolaceTransactionManager;
import cris.prs.messaging.solace.transaction.SolaceTransactionUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Binds a {@link SolaceListenerEndpoint} to the broker and dispatches received messages to a
 * {@link SolaceMessageListener}.
 *
 * <p>Three binding modes are supported (see {@link EndpointMode}):</p>
 * <ul>
 *   <li><b>DURABLE_QUEUE</b> &mdash; the queue is provisioned if missing and the configured topics
 *       are added as subscriptions; {@code concurrency} flows are bound to it.</li>
 *   <li><b>NON_DURABLE_QUEUE</b> &mdash; a temporary queue is created for this client and removed by
 *       the broker on disconnect; ideal for per-pod reply endpoints.</li>
 *   <li><b>DIRECT</b> &mdash; a non-persistent topic subscription on a dedicated session, with no
 *       endpoint, acknowledgements or transactions.</li>
 * </ul>
 *
 * <p>When {@code transactional} is set, each flow is created from its own transacted session which
 * the container binds to the thread before invoking the listener, so that the acknowledgement of
 * the consumed message and anything the listener publishes commit as one unit.</p>
 *
 * <h2>Dispatch modes</h2>
 * <p>{@link ContainerProperties.DispatchMode#INLINE} (the default) invokes the listener on the JCSMP
 * delivery thread &mdash; the lowest latency path, and the only one that is correct for transacted
 * flows.</p>
 * <p>{@link ContainerProperties.DispatchMode#EXECUTOR} runs one invoker task per flow on a Spring
 * {@code AsyncTaskExecutor}, the way {@code DefaultMessageListenerContainer} does for JMS. The JCSMP
 * delivery thread only hands the message to a bounded queue, so it stays free to receive while the
 * listener works; because the queue blocks when full, the broker's flow control is preserved rather
 * than being replaced by unbounded buffering. Listeners then run on Spring managed, non-daemon
 * threads, which also keeps a consumer-only application alive without
 * {@code solace.listener.keep-alive}.</p>
 */
@Slf4j
public class DefaultSolaceMessageListenerContainer implements SolaceMessageListenerContainer {

    private final SolaceSessionFactory sessionFactory;

    private final SolaceListenerEndpoint endpoint;

    private final ContainerProperties containerProperties;

    private final String instanceId;

    private final AtomicBoolean running = new AtomicBoolean();

    private final List<FlowReceiver> flows = new ArrayList<>();

    private final List<TransactedSession> transactedSessions = new ArrayList<>();

    private final List<FlowInvoker> invokers = new ArrayList<>();

    @Setter
    private SolaceMessageListener messageListener;

    @Setter
    private SolaceListenerErrorHandler errorHandler = (message, exception) ->
            log.error("Listener failed for message on {}",
                    message.getDestination() != null ? message.getDestination().getName() : "unknown", exception);

    @Setter
    private SolaceTransactionManager transactionManager;

    /** Executor used by {@link ContainerProperties.DispatchMode#EXECUTOR}; required in that mode. */
    @Setter
    private AsyncTaskExecutor taskExecutor;

    /** The resolved physical endpoint name, available once the container has started. */
    @Getter
    private volatile String resolvedQueueName;

    private JCSMPSession ownSession;

    private XMLMessageConsumer directConsumer;

    private TransactionTemplate transactionTemplate;

    private boolean keepAliveHeld;

    /** Notified of flow lifecycle events; {@code null} means only the container's own logging. */
    @Setter
    private SolaceFlowListener flowListener;

    /** The most recent flow event on any of this container's flows, or {@code null} before the first. */
    @Getter
    private volatile SolaceFlowEvent lastFlowEvent;

    /** Flows currently reported ACTIVE by the broker, when active flow indication is on. */
    private final AtomicInteger activeFlows = new AtomicInteger();

    /** Flows currently DOWN or RECONNECTING. */
    private final AtomicInteger degradedFlows = new AtomicInteger();

    /**
     * Whether the broker has ever reported an ACTIVE or INACTIVE event on this container.
     *
     * <p>Distinguishes "standby" from "active flow indication is switched off". Without it a
     * non-exclusive container, which never receives these events, would look permanently inactive.</p>
     */
    private volatile boolean activeIndicationSeen;

    /** Receives per-message measurements; {@link SolaceListenerMetrics#NO_OP} unless one is set. */
    @Setter
    private SolaceListenerMetrics listenerMetrics = SolaceListenerMetrics.NO_OP;

    /** Guards the non-durable concurrency warning so it is logged once per container. */
    private final AtomicBoolean nonDurableConcurrencyWarned = new AtomicBoolean();

    /**
     * Create a container.
     *
     * @param sessionFactory      supplies the connection, and keys transactions
     * @param endpoint            what to bind to, with its pattern defaults already applied
     * @param containerProperties defaults for anything the endpoint leaves unset
     * @param instanceId          this instance's id, used in per-instance endpoint and topic names
     */
    public DefaultSolaceMessageListenerContainer(SolaceSessionFactory sessionFactory,
            SolaceListenerEndpoint endpoint, ContainerProperties containerProperties, String instanceId) {
        Assert.notNull(sessionFactory, "'sessionFactory' must not be null");
        Assert.notNull(endpoint, "'endpoint' must not be null");
        Assert.notNull(containerProperties, "'containerProperties' must not be null");
        this.sessionFactory = sessionFactory;
        this.endpoint = endpoint;
        this.containerProperties = containerProperties;
        this.instanceId = instanceId;
        this.messageListener = endpoint.getMessageListener();
    }

    /**
     * How many flows this container currently has bound.
     *
     * <p>Zero until the container starts, and zero again once it stops. Useful as a gauge: a running
     * container whose flow count is below its configured concurrency has lost flows.</p>
     *
     * @return the number of bound flows
     */
    public int getActiveFlowCount() {
        return this.flows.size();
    }

    /** {@inheritDoc} */
    @Override
    public String getListenerId() {
        return this.endpoint.getId();
    }

    /** {@inheritDoc} */
    @Override
    public void setupMessageListener(SolaceMessageListener listener) {
        this.messageListener = listener;
    }

    private EndpointMode endpointMode() {
        return this.endpoint.getEndpointMode() != null
                ? this.endpoint.getEndpointMode()
                : this.containerProperties.getEndpointMode();
    }

    /**
     * The number of flows this container actually binds.
     *
     * <p>A non-durable queue is a temporary endpoint owned by this client and accepts exactly one
     * flow, whatever access type was requested; binding a second one is rejected with
     * {@code 503 Max clients exceeded for queue}. A configured concurrency above 1 is therefore
     * clamped to 1 for that mode, with a warning logged once. Consume in parallel by using a durable
     * queue with a non-exclusive access type instead.</p>
     *
     * @return the effective flow count, never less than 1
     */
    private int concurrency() {
        int configured = this.endpoint.getConcurrency() != null
                ? this.endpoint.getConcurrency()
                : this.containerProperties.getConcurrency();
        if (configured > 1 && endpointMode() == EndpointMode.NON_DURABLE_QUEUE) {
            if (this.nonDurableConcurrencyWarned.compareAndSet(false, true)) {
                log.warn("Container '{}' asks for {} flows on a non-durable queue. A temporary "
                        + "endpoint accepts exactly one flow and rejects the rest with '503 Max "
                        + "clients exceeded for queue', so concurrency is clamped to 1. Use a "
                        + "durable queue with a non-exclusive access type to consume in parallel.",
                        getListenerId(), configured);
            }
            return 1;
        }
        return configured;
    }

    /**
     * What to do with a message whose listener threw.
     *
     * <p>The endpoint's override wins, then the container's {@code errorOutcome}, then the deprecated
     * {@code ackOnError} &mdash; {@code true} as {@code ACCEPTED}, {@code false} as {@code NONE}.</p>
     *
     * @return the outcome to apply when no error handler overrides it, never {@code null}
     */
    private SettlementOutcome errorOutcome() {
        if (this.endpoint.getErrorOutcome() != null) {
            return this.endpoint.getErrorOutcome();
        }
        if (this.containerProperties.getErrorOutcome() != null) {
            return this.containerProperties.getErrorOutcome();
        }
        return this.containerProperties.isAckOnError()
                ? SettlementOutcome.ACCEPTED
                : SettlementOutcome.NONE;
    }

    /**
     * Whether this container's flows negotiate the negative settlement outcomes at bind time.
     *
     * <p>Explicit configuration wins; otherwise it is derived from the resolved outcome, since a
     * container that will never send {@code FAILED} or {@code REJECTED} should not ask the broker for
     * capabilities it does not need.</p>
     *
     * @return {@code true} to request {@code FAILED} and {@code REJECTED} on every flow
     */
    private boolean negotiateSettlementOutcomes() {
        Boolean configured = this.containerProperties.getNegativeAcknowledgement();
        if (configured != null) {
            return configured;
        }
        return errorOutcome().requiresNegotiation();
    }

    private boolean transactional() {
        return this.endpoint.getTransactional() != null
                ? this.endpoint.getTransactional()
                : this.containerProperties.isTransactional();
    }

    private ContainerProperties.DispatchMode dispatch() {
        return this.endpoint.getDispatch() != null
                ? this.endpoint.getDispatch()
                : this.containerProperties.getDispatch();
    }

    /**
     * {@inheritDoc}
     *
     * @return the endpoint's setting when it has one, otherwise the container default
     */
    @Override
    public boolean isAutoStartup() {
        return this.endpoint.getAutoStartup() != null
                ? this.endpoint.getAutoStartup()
                : this.containerProperties.isAutoStartup();
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code containerProperties.phase}, by default {@code Integer.MAX_VALUE - 100}
     */
    @Override
    public int getPhase() {
        return this.containerProperties.getPhase();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    /**
     * Validate the configuration, bind the endpoint and start consuming.
     *
     * <p>Idempotent: a second call on a running container does nothing. If any step fails, everything
     * already opened is released before the exception propagates, so a failed start leaves no flows
     * or sessions behind.</p>
     *
     * @throws IllegalStateException    if the configuration is inconsistent &mdash; no listener, a
     *                                  transactional container without a transaction manager or on a
     *                                  direct endpoint, a concurrency exceeding the transacted-session
     *                                  limit, or {@code EXECUTOR} dispatch combined with transactions
     * @throws SolaceMessagingException if the broker rejects provisioning, subscribing or binding
     */
    @Override
    public void start() {
        if (!this.running.compareAndSet(false, true)) {
            return;
        }
        Assert.state(this.messageListener != null,
                "No message listener configured for container '" + getListenerId() + "'");
        try {
            if (transactional()) {
                Assert.state(this.transactionManager != null,
                        "A SolaceTransactionManager is required for transactional container '"
                                + getListenerId() + "'");
                Assert.state(endpointMode().isQueueBased(),
                        "Transactions require a queue based endpoint mode for container '"
                                + getListenerId() + "'");
                this.transactionTemplate = new TransactionTemplate(this.transactionManager);
                int maxTransactedSessions = this.containerProperties.getMaxTransactedSessionsPerConnection();
                Assert.state(concurrency() <= maxTransactedSessions,
                        "Container '" + getListenerId() + "' needs one transacted session per flow, "
                                + "but concurrency is " + concurrency() + " and Solace allows "
                                + maxTransactedSessions + " transacted sessions per connection. "
                                + "Lower the concurrency, or raise max-transacted-sessions on the "
                                + "broker's client profile and solace.listener."
                                + "max-transacted-sessions-per-connection to match.");
            }
            if (dispatch() == ContainerProperties.DispatchMode.EXECUTOR) {
                Assert.state(this.taskExecutor != null,
                        "An AsyncTaskExecutor is required for EXECUTOR dispatch on container '"
                                + getListenerId() + "'");
                // A transacted session's commit acknowledges every message delivered on it so far,
                // not merely the one being handled. Buffering messages away from the delivery thread
                // would therefore let a commit cover messages that have not been processed yet, and
                // a rollback redeliver ones that have. Transacted flows stay strictly inline.
                Assert.state(!transactional(), "EXECUTOR dispatch cannot be combined with "
                        + "transactional=true on container '" + getListenerId() + "': a Solace "
                        + "transacted session must be driven by the thread its messages are "
                        + "delivered on. Use dispatch=INLINE, or keep-alive, for transactional "
                        + "containers.");
            }
            if (endpointMode() == EndpointMode.DIRECT) {
                startDirect();
            }
            else {
                startQueue();
            }
            if (this.containerProperties.isKeepAlive()) {
                ContainerKeepAlive.acquire();
                this.keepAliveHeld = true;
            }
            log.info("Started Solace listener container '{}' [pattern={}, mode={}, endpoint={}, topics={}, concurrency={}, transactional={}, dispatch={}]",
                    getListenerId(), this.endpoint.getPattern(), endpointMode(), this.resolvedQueueName,
                    this.endpoint.resolveTopics(this.instanceId), concurrency(), transactional(), dispatch());
        }
        catch (JCSMPException | RuntimeException ex) {
            // Roll back a partially started container: flows and sessions bound so far would
            // otherwise stay open for the life of the JVM.
            releaseResources();
            this.running.set(false);
            throw new SolaceMessagingException("Unable to start Solace listener container '"
                    + getListenerId() + "'", ex);
        }
    }

    private void startDirect() throws JCSMPException {
        // A dedicated session keeps this container's direct subscriptions isolated from others.
        this.ownSession = this.sessionFactory.createSession();
        this.directConsumer = this.ownSession.getMessageConsumer(
                new ContainerMessageListener(null, newInvokerIfNeeded(0)));
        for (String topicName : this.endpoint.resolveTopics(this.instanceId)) {
            addSubscription(this.ownSession, null, JCSMPFactory.onlyInstance().createTopic(topicName));
        }
        this.directConsumer.start();
    }

    private void startQueue() throws JCSMPException {
        JCSMPSession session = this.sessionFactory.getSharedSession();
        // The exchange pattern decides exclusive (fan-out, one endpoint per instance) versus
        // non-exclusive (competing consumers on a shared endpoint).
        EndpointProperties endpointProperties = this.containerProperties.getEndpoint()
                .toEndpointProperties(this.endpoint.getAccessType());
        String queueName = this.endpoint.resolveQueueName(this.instanceId);
        Queue queue;
        if (endpointMode() == EndpointMode.NON_DURABLE_QUEUE) {
            queue = session.createTemporaryQueue(queueName);
        }
        else {
            queue = JCSMPFactory.onlyInstance().createQueue(queueName);
            if (this.containerProperties.isProvisionEndpoint()) {
                provisionDeadMessageQueue(session);
                provision(session, queue, endpointProperties, "queue");
            }
        }
        this.resolvedQueueName = queue.getName();

        // Bind the flows before subscribing. A temporary queue is only created on the broker when a
        // flow binds to it, so adding a subscription first fails with 503 Unknown Queue. Flows are
        // created stopped, so nothing is delivered until the subscriptions are in place.
        int concurrency = concurrency();
        ContainerProperties.AccessType accessType = this.endpoint.getAccessType() != null
                ? this.endpoint.getAccessType()
                : this.containerProperties.getEndpoint().getAccessType();
        if (accessType == ContainerProperties.AccessType.EXCLUSIVE && concurrency > 1) {
            log.warn("Container '{}' binds {} flows to the exclusive endpoint '{}'. Only one can be "
                    + "active; the rest are standby at best, and a temporary endpoint rejects them "
                    + "with '503 Max clients exceeded for queue'. Set concurrency to 1, or use a "
                    + "non-exclusive endpoint to consume in parallel.",
                    getListenerId(), concurrency, this.resolvedQueueName);
        }
        for (int i = 0; i < concurrency; i++) {
            ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
            flowProperties.setEndpoint(queue);
            flowProperties.setStartState(false);
            if (negotiateSettlementOutcomes() && !transactional()) {
                // A flow may only send an outcome it asked for when it bound. Requesting both keeps
                // a per-message decision by an error handler possible without rebinding.
                flowProperties.addRequiredSettlementOutcomes(
                        XMLMessage.Outcome.FAILED, XMLMessage.Outcome.REJECTED);
            }
            if (StringUtils.hasText(this.endpoint.getSelector())) {
                flowProperties.setSelector(this.endpoint.getSelector());
            }
            this.containerProperties.getFlow().applyTo(flowProperties,
                    accessType == ContainerProperties.AccessType.EXCLUSIVE);
            FlowEventHandler eventHandler = flowEventHandler(i);
            FlowReceiver flow;
            if (transactional()) {
                TransactedSession transactedSession =
                        this.sessionFactory.createTransactedSession(transactedConnection());
                this.transactedSessions.add(transactedSession);
                SolaceResourceHolder holder = new SolaceResourceHolder(transactedSession, true);
                flow = transactedSession.createFlow(new ContainerMessageListener(holder, null),
                        flowProperties, endpointProperties, eventHandler);
            }
            else {
                flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                flow = session.createFlow(new ContainerMessageListener(null, newInvokerIfNeeded(i)),
                        flowProperties, endpointProperties, eventHandler);
            }
            this.flows.add(flow);
        }

        for (String topicName : this.endpoint.resolveTopics(this.instanceId)) {
            addSubscription(session, queue, JCSMPFactory.onlyInstance().createTopic(topicName));
        }

        for (FlowReceiver flow : this.flows) {
            flow.start();
        }
    }

    /**
     * Build the JCSMP event handler for one flow.
     *
     * <p>Every event is logged, counted and forwarded to the container's {@link SolaceFlowListener}
     * if it has one. Log levels are chosen by what an operator needs to see: {@code DOWN} is an error
     * because the flow will not come back without a restart, {@code RECONNECTING} is a warning
     * because consumption has stopped for now, and the rest are informational.</p>
     *
     * @param flowIndex which of this container's flows the handler belongs to
     * @return the handler to pass to {@code createFlow}
     */
    private FlowEventHandler flowEventHandler(int flowIndex) {
        return (source, args) -> handleFlowEvent(flowIndex, args);
    }

    /**
     * Record and report one flow event.
     *
     * <p>Runs on a JCSMP notification thread. Everything here is guarded, because an exception
     * escaping into JCSMP is dropped silently and would leave the container's own state
     * inconsistent.</p>
     *
     * @param flowIndex which of this container's flows raised the event
     * @param args      the JCSMP event
     */
    private void handleFlowEvent(int flowIndex, FlowEventArgs args) {
        SolaceFlowEvent event = SolaceFlowEvent.from(args.getEvent());
        SolaceFlowEvent previous = this.lastFlowEvent;
        this.lastFlowEvent = event;

        switch (event) {
            case ACTIVE -> {
                this.activeIndicationSeen = true;
                this.activeFlows.incrementAndGet();
            }
            case INACTIVE -> {
                this.activeIndicationSeen = true;
                this.activeFlows.updateAndGet(count -> Math.max(0, count - 1));
            }
            case DOWN, RECONNECTING -> {
                if (previous == null || !previous.isDegraded()) {
                    this.degradedFlows.incrementAndGet();
                }
            }
            case UP, RECONNECTED -> this.degradedFlows.updateAndGet(count -> Math.max(0, count - 1));
            default -> { }
        }

        switch (event) {
            case DOWN -> log.error("Flow {} of container '{}' on endpoint '{}' is DOWN and will not "
                            + "recover on its own; the container must be restarted to consume again. {}",
                    flowIndex, getListenerId(), this.resolvedQueueName, args.getInfo(),
                    args.getException());
            case RECONNECTING -> log.warn("Flow {} of container '{}' on endpoint '{}' is "
                            + "reconnecting; consumption has stopped. {}",
                    flowIndex, getListenerId(), this.resolvedQueueName, args.getInfo());
            case RECONNECTED -> log.info("Flow {} of container '{}' reconnected to endpoint '{}'",
                    flowIndex, getListenerId(), this.resolvedQueueName);
            case ACTIVE -> log.info("Flow {} of container '{}' is now the ACTIVE consumer on '{}'",
                    flowIndex, getListenerId(), this.resolvedQueueName);
            case INACTIVE -> log.info("Flow {} of container '{}' is now standing by on '{}'",
                    flowIndex, getListenerId(), this.resolvedQueueName);
            default -> log.debug("Flow {} of container '{}' event {} on '{}': {}", flowIndex,
                    getListenerId(), event, this.resolvedQueueName, args.getInfo());
        }

        try {
            this.listenerMetrics.recordFlowEvent(getListenerId(), event.name());
        }
        catch (RuntimeException ex) {
            log.debug("Listener metrics failed for container '{}'", getListenerId(), ex);
        }

        SolaceFlowListener listener = this.flowListener;
        if (listener == null) {
            return;
        }
        try {
            listener.onFlowEvent(new SolaceFlowEventArgs(getListenerId(), flowIndex,
                    this.resolvedQueueName, event, args.getInfo(), args.getException(),
                    args.getResponseCode()));
        }
        catch (RuntimeException ex) {
            log.warn("Flow listener of container '{}' threw on a {} event", getListenerId(), event, ex);
        }
    }

    /**
     * Whether this container is the active consumer.
     *
     * <p>On an exclusive endpoint with active flow indication on, this is {@code true} only on the
     * instance the broker has made the consumer &mdash; which makes it a leader-election primitive
     * with no extra coordination. Everywhere else it simply mirrors {@link #isRunning()}, because a
     * non-exclusive flow is always active when it is running.</p>
     *
     * @return {@code true} when this container is consuming as the active flow
     */
    public boolean isActive() {
        if (!isRunning() || isDegraded()) {
            return false;
        }
        return !this.activeIndicationSeen || this.activeFlows.get() > 0;
    }

    /**
     * Whether any of this container's flows is down or reconnecting.
     *
     * <p>The difference between "running" and "actually consuming": a container stays running through
     * a reconnect, so {@link #isRunning()} alone cannot tell a health check that delivery has
     * stopped.</p>
     *
     * @return {@code true} when at least one flow is degraded
     */
    public boolean isDegraded() {
        return this.degradedFlows.get() > 0;
    }

    /**
     * The connection this container's transacted sessions are taken from.
     *
     * <p>Solace caps transacted sessions per client connection, so a transactional container opens
     * its own connection rather than competing for the shared session's allowance with every other
     * container in the application. Two transactional listeners at concurrency 10 and 5 need 15
     * transacted sessions between them, which no single connection will give at the default limit
     * of 10.</p>
     */
    private JCSMPSession transactedConnection() {
        if (this.ownSession == null) {
            this.ownSession = this.sessionFactory.createSession();
        }
        return this.ownSession;
    }

    /**
     * Create and start the invoker for one flow when EXECUTOR dispatch is in use, otherwise return
     * {@code null} so that the flow invokes the listener inline.
     */
    private FlowInvoker newInvokerIfNeeded(int index) {
        if (dispatch() != ContainerProperties.DispatchMode.EXECUTOR) {
            return null;
        }
        FlowInvoker invoker = new FlowInvoker(index, this.containerProperties.getDispatchQueueCapacity());
        this.invokers.add(invoker);
        this.taskExecutor.execute(invoker);
        return invoker;
    }

    /**
     * Create the message VPN's dead message queue if it is missing, so that messages exhausting
     * {@code max-redelivery-count} have somewhere to land instead of being discarded.
     */
    private void provisionDeadMessageQueue(JCSMPSession session) throws JCSMPException {
        ContainerProperties.DeadMessageQueue dmq = this.containerProperties.getEndpoint().getDeadMessageQueue();
        if (!dmq.isProvision()) {
            return;
        }
        provision(session, JCSMPFactory.onlyInstance().createQueue(dmq.getName()),
                dmq.toEndpointProperties(), "dead message queue");
    }

    /**
     * Provision an endpoint, reporting rather than hiding the case where it already exists.
     *
     * <p>The broker never reconfigures an existing endpoint on provision, so settings such as
     * {@code max-redelivery-count} and the quota only take effect the first time an endpoint is
     * created. Saying so in the log beats silently ignoring the mismatch, which makes a queue look
     * configured when it is not.</p>
     */
    private void provision(JCSMPSession session, Endpoint endpoint, EndpointProperties properties,
            String description) throws JCSMPException {
        try {
            session.provision(endpoint, properties, JCSMPSession.WAIT_FOR_CONFIRM);
            log.info("Provisioned {} '{}' for container '{}'", description, endpoint.getName(),
                    getListenerId());
        }
        catch (JCSMPErrorResponseException ex) {
            int subcode = ex.getSubcodeEx();
            if (subcode == JCSMPErrorResponseSubcodeEx.ENDPOINT_ALREADY_EXISTS) {
                log.debug("The {} '{}' already exists", description, endpoint.getName());
            }
            else if (subcode == JCSMPErrorResponseSubcodeEx.ENDPOINT_PROPERTY_MISMATCH) {
                log.warn("The {} '{}' already exists with different properties, and the broker keeps "
                        + "the ones it has. Endpoint settings such as max-redelivery-count and quota "
                        + "are only applied when the endpoint is first created: delete it on the "
                        + "broker, or change it through the admin UI or SEMP, for '{}' to take effect.",
                        description, endpoint.getName(), getListenerId());
            }
            else {
                throw ex;
            }
        }
    }

    /**
     * Add a topic subscription, tolerating one that is already there.
     *
     * <p>A durable endpoint keeps its subscriptions between runs, so on every restart after the
     * first the broker answers {@code 400 Subscription Already Exists}. That is the expected steady
     * state, not a failure &mdash; the same reasoning as {@code FLAG_IGNORE_ALREADY_EXISTS} when
     * provisioning the endpoint itself. A subscription that exists with <em>different</em>
     * properties still propagates, because that one really is a misconfiguration.</p>
     *
     * @param queue the endpoint to subscribe, or {@code null} to subscribe the session (DIRECT mode)
     */
    private void addSubscription(JCSMPSession session, Queue queue, Topic topic) throws JCSMPException {
        try {
            if (queue == null) {
                session.addSubscription(topic, true);
            }
            else {
                session.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);
            }
        }
        catch (JCSMPErrorResponseException ex) {
            if (ex.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                throw ex;
            }
            log.debug("Subscription '{}' is already present on {}", topic.getName(),
                    queue != null ? queue.getName() : "the session");
        }
    }

    /**
     * Stop consuming and release everything this container holds.
     *
     * <p>Idempotent, and safe to call on a container that never started.</p>
     */
    @Override
    public void stop() {
        if (!this.running.compareAndSet(true, false)) {
            return;
        }
        releaseResources();
        log.info("Stopped Solace listener container '{}'", getListenerId());
    }

    /**
     * Close flows, invokers, transacted sessions and any session this container owns. Idempotent.
     *
     * <p>Delivery is halted first, then the invokers are given {@code shutdownTimeout} to drain what
     * they have already buffered, and only then are the flows closed &mdash; acknowledging a message
     * on a closed flow would fail.</p>
     */
    private void releaseResources() {
        this.flows.forEach(flow -> {
            try {
                flow.stop();
            }
            catch (Exception ex) {
                log.debug("Error stopping Solace flow for container '{}'", getListenerId(), ex);
            }
        });
        Duration shutdownTimeout = this.containerProperties.getShutdownTimeout();
        this.invokers.forEach(invoker -> invoker.stop(shutdownTimeout));
        this.invokers.clear();
        this.flows.forEach(flow -> {
            try {
                flow.close();
            }
            catch (Exception ex) {
                log.debug("Error closing Solace flow for container '{}'", getListenerId(), ex);
            }
        });
        this.flows.clear();
        this.activeFlows.set(0);
        this.degradedFlows.set(0);
        this.activeIndicationSeen = false;
        this.lastFlowEvent = null;
        this.transactedSessions.forEach(transactedSession -> {
            try {
                transactedSession.close();
            }
            catch (Exception ex) {
                log.debug("Error closing transacted session for container '{}'", getListenerId(), ex);
            }
        });
        this.transactedSessions.clear();
        if (this.directConsumer != null) {
            this.directConsumer.close();
            this.directConsumer = null;
        }
        if (this.ownSession != null) {
            this.sessionFactory.closeSession(this.ownSession);
            this.ownSession = null;
        }
        if (this.keepAliveHeld) {
            this.keepAliveHeld = false;
            ContainerKeepAlive.release();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param callback run once the container has stopped
     */
    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    /** Invoke the listener and acknowledge, or hand the failure to the error handler. */
    private void invokeListener(BytesXMLMessage message) {
        long startedAt = System.nanoTime();
        try {
            this.messageListener.onMessage(message);
            recordSuccess(startedAt);
            message.ackMessage();
        }
        catch (Exception ex) {
            recordFailure(startedAt, ex);
            this.errorHandler.handleError(message, ex);
            settle(message, ex);
        }
    }

    /**
     * Apply the settlement outcome for a failed message.
     *
     * <p>The error handler is asked first, so a per-failure policy can override the container's
     * blanket one; {@code null} from the handler means "you decide". {@code NONE} sends nothing and
     * leaves the message for redelivery on the next bind.</p>
     *
     * <p>A failure to settle is logged rather than rethrown. Rethrowing here would propagate into the
     * JCSMP delivery thread, where nothing useful can be done with it, and the message is already in
     * a state the broker will resolve by redelivering it.</p>
     *
     * @param message the message whose listener threw
     * @param cause   what it threw
     */
    private void settle(BytesXMLMessage message, Exception cause) {
        SettlementOutcome outcome = resolveOutcome(message, cause);
        if (outcome == SettlementOutcome.NONE) {
            recordSettlement(outcome);
            return;
        }
        try {
            if (outcome == SettlementOutcome.ACCEPTED) {
                message.ackMessage();
            }
            else {
                message.settle(outcome.jcsmpOutcome());
            }
            recordSettlement(outcome);
        }
        catch (Exception ex) {
            log.error("Container '{}' could not settle a message as {}. The flow must negotiate "
                    + "FAILED and REJECTED at bind time: set "
                    + "solace.listener.negative-acknowledgement=true, and check the broker supports "
                    + "settlement outcomes. The message is left for redelivery.",
                    getListenerId(), outcome, ex);
        }
    }

    /**
     * Ask the error handler for an outcome, falling back to the configured one.
     *
     * @param message the message whose listener threw
     * @param cause   what it threw
     * @return the outcome to apply, never {@code null}
     */
    private SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception cause) {
        try {
            SettlementOutcome resolved = this.errorHandler.resolveOutcome(message, cause);
            if (resolved != null) {
                return resolved;
            }
        }
        catch (RuntimeException ex) {
            log.warn("Error handler of container '{}' failed to resolve an outcome; using the "
                    + "configured one", getListenerId(), ex);
        }
        return errorOutcome();
    }

    /** Report a settlement decision. Guarded like {@link #recordReceived()}. */
    private void recordSettlement(SettlementOutcome outcome) {
        try {
            this.listenerMetrics.recordSettlement(getListenerId(), outcome.name());
        }
        catch (RuntimeException ex) {
            log.debug("Listener metrics failed for container '{}'", getListenerId(), ex);
        }
    }

    /**
     * Report a delivery to the metrics collaborator.
     *
     * <p>Every call is guarded: instrumentation must never be able to fail a message, so an
     * exception here is logged at debug and swallowed.</p>
     */
    private void recordReceived() {
        try {
            this.listenerMetrics.recordReceived(getListenerId());
        }
        catch (RuntimeException ex) {
            log.debug("Listener metrics failed for container '{}'", getListenerId(), ex);
        }
    }

    /** Report a successful invocation. Guarded like {@link #recordReceived()}. */
    private void recordSuccess(long startedAt) {
        try {
            this.listenerMetrics.recordSuccess(getListenerId(), System.nanoTime() - startedAt);
        }
        catch (RuntimeException ex) {
            log.debug("Listener metrics failed for container '{}'", getListenerId(), ex);
        }
    }

    /** Report a failed invocation. Guarded like {@link #recordReceived()}. */
    private void recordFailure(long startedAt, Exception exception) {
        try {
            this.listenerMetrics.recordFailure(getListenerId(), System.nanoTime() - startedAt, exception);
        }
        catch (RuntimeException ex) {
            log.debug("Listener metrics failed for container '{}'", getListenerId(), ex);
        }
    }

    /**
     * One invoker task per flow, submitted to the {@code AsyncTaskExecutor}, mirroring the invoker
     * tasks of Spring's {@code DefaultMessageListenerContainer}.
     *
     * <p>The hand-off queue is bounded and {@link BlockingQueue#put} blocks, so a slow listener
     * pushes back onto the JCSMP delivery thread and from there onto the broker's transport window.
     * Replacing that with an unbounded queue would trade flow control for heap.</p>
     */
    private final class FlowInvoker implements Runnable {

        private final int index;

        private final BlockingQueue<BytesXMLMessage> handoff;

        private final CountDownLatch stopped = new CountDownLatch(1);

        private volatile boolean active = true;

        private volatile Thread workerThread;

        private FlowInvoker(int index, int capacity) {
            this.index = index;
            this.handoff = new LinkedBlockingQueue<>(Math.max(1, capacity));
        }

        /** Called on the JCSMP delivery thread; blocks while the invoker is saturated. */
        void submit(BytesXMLMessage message) {
            try {
                this.handoff.put(message);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            this.workerThread = Thread.currentThread();
            Thread.currentThread().setName("solace-" + getListenerId() + "-" + this.index);
            try {
                while (this.active) {
                    BytesXMLMessage message = this.handoff.poll(200, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        invokeListener(message);
                    }
                }
                // Drain whatever was buffered when the stop was requested.
                BytesXMLMessage message;
                while ((message = this.handoff.poll()) != null) {
                    invokeListener(message);
                }
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            finally {
                this.stopped.countDown();
            }
        }

        void stop(Duration timeout) {
            this.active = false;
            try {
                if (!this.stopped.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("Invoker {} of container '{}' did not finish within {}; interrupting",
                            this.index, getListenerId(), timeout);
                    Thread thread = this.workerThread;
                    if (thread != null) {
                        thread.interrupt();
                    }
                }
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Dispatches a received message: to the flow's invoker when EXECUTOR dispatch is configured,
     * otherwise inline &mdash; either directly (acknowledging on success) or inside a Solace local
     * transaction when the flow is transacted.
     */
    private final class ContainerMessageListener implements XMLMessageListener {

        private final SolaceResourceHolder resourceHolder;

        private final FlowInvoker invoker;

        private ContainerMessageListener(SolaceResourceHolder resourceHolder, FlowInvoker invoker) {
            this.resourceHolder = resourceHolder;
            this.invoker = invoker;
        }

        @Override
        public void onReceive(BytesXMLMessage message) {
            recordReceived();
            if (this.invoker != null) {
                this.invoker.submit(message);
            }
            else if (this.resourceHolder != null) {
                receiveInTransaction(message);
            }
            else {
                invokeListener(message);
            }
        }

        private void receiveInTransaction(BytesXMLMessage message) {
            long startedAt = System.nanoTime();
            SolaceTransactionUtils.bindResourceHolder(
                    DefaultSolaceMessageListenerContainer.this.sessionFactory, this.resourceHolder);
            try {
                DefaultSolaceMessageListenerContainer.this.transactionTemplate.executeWithoutResult(status -> {
                    try {
                        DefaultSolaceMessageListenerContainer.this.messageListener.onMessage(message);
                    }
                    catch (RuntimeException ex) {
                        throw ex;
                    }
                    catch (Exception ex) {
                        throw new SolaceMessagingException("Listener invocation failed", ex);
                    }
                });
                recordSuccess(startedAt);
            }
            catch (Exception ex) {
                // The transaction has already been rolled back; the broker will redeliver.
                recordFailure(startedAt, ex);
                DefaultSolaceMessageListenerContainer.this.errorHandler.handleError(message, ex);
            }
            finally {
                SolaceTransactionUtils.unbindResourceHolder(
                        DefaultSolaceMessageListenerContainer.this.sessionFactory);
            }
        }

        @Override
        public void onException(JCSMPException exception) {
            log.error("Solace consumer error in container '{}'", getListenerId(), exception);
        }
    }
}
