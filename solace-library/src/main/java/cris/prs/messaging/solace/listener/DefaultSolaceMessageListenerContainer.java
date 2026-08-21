package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import cris.prs.messaging.solace.core.EndpointMode;
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
 * {@link ContainerProperties#isKeepAlive()}.</p>
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

    @Override
    public String getListenerId() {
        return this.endpoint.getId();
    }

    @Override
    public void setupMessageListener(SolaceMessageListener listener) {
        this.messageListener = listener;
    }

    private EndpointMode endpointMode() {
        return this.endpoint.getEndpointMode() != null
                ? this.endpoint.getEndpointMode()
                : this.containerProperties.getEndpointMode();
    }

    private int concurrency() {
        return this.endpoint.getConcurrency() != null
                ? this.endpoint.getConcurrency()
                : this.containerProperties.getConcurrency();
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

    @Override
    public boolean isAutoStartup() {
        return this.endpoint.getAutoStartup() != null
                ? this.endpoint.getAutoStartup()
                : this.containerProperties.isAutoStartup();
    }

    @Override
    public int getPhase() {
        return this.containerProperties.getPhase();
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

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
            log.info("Started Solace listener container '{}' [mode={}, endpoint={}, topics={}, concurrency={}, transactional={}, dispatch={}]",
                    getListenerId(), endpointMode(), this.resolvedQueueName,
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
        EndpointProperties endpointProperties = this.containerProperties.getEndpoint().toEndpointProperties();
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
        for (int i = 0; i < concurrency; i++) {
            ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
            flowProperties.setEndpoint(queue);
            flowProperties.setStartState(false);
            if (StringUtils.hasText(this.endpoint.getSelector())) {
                flowProperties.setSelector(this.endpoint.getSelector());
            }
            FlowReceiver flow;
            if (transactional()) {
                TransactedSession transactedSession = this.sessionFactory.createTransactedSession();
                this.transactedSessions.add(transactedSession);
                SolaceResourceHolder holder = new SolaceResourceHolder(transactedSession, true);
                flow = transactedSession.createFlow(new ContainerMessageListener(holder, null),
                        flowProperties, endpointProperties);
            }
            else {
                flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                flow = session.createFlow(new ContainerMessageListener(null, newInvokerIfNeeded(i)),
                        flowProperties, endpointProperties);
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

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    /** Invoke the listener and acknowledge, or hand the failure to the error handler. */
    private void invokeListener(BytesXMLMessage message) {
        try {
            this.messageListener.onMessage(message);
            message.ackMessage();
        }
        catch (Exception ex) {
            this.errorHandler.handleError(message, ex);
            if (this.containerProperties.isAckOnError()) {
                message.ackMessage();
            }
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
            }
            catch (Exception ex) {
                // The transaction has already been rolled back; the broker will redeliver.
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
