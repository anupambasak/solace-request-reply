package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
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
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
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

    @Setter
    private SolaceMessageListener messageListener;

    @Setter
    private SolaceListenerErrorHandler errorHandler = (message, exception) ->
            log.error("Listener failed for message on {}",
                    message.getDestination() != null ? message.getDestination().getName() : "unknown", exception);

    @Setter
    private SolaceTransactionManager transactionManager;

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
            log.info("Started Solace listener container '{}' [mode={}, endpoint={}, topics={}, concurrency={}, transactional={}]",
                    getListenerId(), endpointMode(), this.resolvedQueueName,
                    this.endpoint.resolveTopics(this.instanceId), concurrency(), transactional());
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
        this.directConsumer = this.ownSession.getMessageConsumer(new ContainerMessageListener(null));
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
                session.provision(queue, endpointProperties,
                        JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);
            }
        }
        this.resolvedQueueName = queue.getName();

        for (String topicName : this.endpoint.resolveTopics(this.instanceId)) {
            addSubscription(session, queue, JCSMPFactory.onlyInstance().createTopic(topicName));
        }

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
                flow = transactedSession.createFlow(new ContainerMessageListener(holder), flowProperties,
                        endpointProperties);
            }
            else {
                flowProperties.setAckMode(com.solacesystems.jcsmp.JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                flow = session.createFlow(new ContainerMessageListener(null), flowProperties, endpointProperties);
            }
            this.flows.add(flow);
            flow.start();
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

    /** Close flows, transacted sessions and any session this container owns. Idempotent. */
    private void releaseResources() {
        this.flows.forEach(flow -> {
            try {
                flow.stop();
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

    /**
     * Dispatches a received message, either directly (acknowledging on success) or inside a Solace
     * local transaction when the flow is transacted.
     */
    private final class ContainerMessageListener implements XMLMessageListener {

        private final SolaceResourceHolder resourceHolder;

        private ContainerMessageListener(SolaceResourceHolder resourceHolder) {
            this.resourceHolder = resourceHolder;
        }

        @Override
        public void onReceive(BytesXMLMessage message) {
            if (this.resourceHolder != null) {
                receiveInTransaction(message);
            }
            else {
                receive(message);
            }
        }

        private void receive(BytesXMLMessage message) {
            try {
                DefaultSolaceMessageListenerContainer.this.messageListener.onMessage(message);
                message.ackMessage();
            }
            catch (Exception ex) {
                DefaultSolaceMessageListenerContainer.this.errorHandler.handleError(message, ex);
                if (DefaultSolaceMessageListenerContainer.this.containerProperties.isAckOnError()) {
                    message.ackMessage();
                }
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
