package cris.prs.messaging.solace.listener;

import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.core.SolaceTemplate;
import cris.prs.messaging.solace.support.InstanceIdProvider;
import lombok.Setter;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Default container factory &mdash; the Solace equivalent of
 * {@code ConcurrentKafkaListenerContainerFactory}. Registered by the auto-configuration under the
 * bean name {@code solaceListenerContainerFactory}.
 */
public class DefaultSolaceListenerContainerFactory implements SolaceListenerContainerFactory {

    private final SolaceSessionFactory sessionFactory;

    private final SolaceMessageConverter messageConverter;

    private final SolaceHeaderMapper headerMapper;

    private final InstanceIdProvider instanceIdProvider;

    private final ContainerProperties containerProperties;

    /** Required for {@code transactional} containers. */
    @Setter
    private cris.prs.messaging.solace.transaction.SolaceTransactionManager transactionManager;

    /** Used to publish listener return values as replies. */
    @Setter
    private SolaceTemplate<Object> replyTemplate;

    @Setter
    private SolaceListenerErrorHandler errorHandler;

    /** Executor backing {@link ContainerProperties.DispatchMode#EXECUTOR}. */
    @Setter
    private org.springframework.core.task.AsyncTaskExecutor taskExecutor;

    /**
     * Given to every container this factory creates, so that message handling is measured.
     *
     * <p>Defaults to {@link SolaceListenerMetrics#NO_OP}; the Micrometer implementation is supplied
     * by auto-configuration when a {@code MeterRegistry} is present.</p>
     */
    @Setter
    private SolaceListenerMetrics listenerMetrics = SolaceListenerMetrics.NO_OP;

    /**
     * Given to every container this factory creates, so flow lifecycle events reach the application.
     *
     * <p>{@code null} leaves the container's own logging as the only reporting.</p>
     */
    @Setter
    private SolaceFlowListener flowListener;

    /**
     * Create a container factory.
     *
     * @param sessionFactory      supplies connections to every container built here
     * @param messageConverter    converts message bodies
     * @param headerMapper        maps headers
     * @param instanceIdProvider  supplies the id used in per-instance endpoint names
     * @param containerProperties defaults shared by every container from this factory; an endpoint may
     *                            override them individually
     */
    public DefaultSolaceListenerContainerFactory(SolaceSessionFactory sessionFactory,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper,
            InstanceIdProvider instanceIdProvider, ContainerProperties containerProperties) {
        Assert.notNull(sessionFactory, "'sessionFactory' must not be null");
        this.sessionFactory = sessionFactory;
        this.messageConverter = messageConverter;
        this.headerMapper = headerMapper;
        this.instanceIdProvider = instanceIdProvider;
        this.containerProperties = containerProperties;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Builds a {@link MethodSolaceListenerAdapter} for an annotated method, or uses the endpoint's
     * own listener, then wires the container with the shared collaborators.</p>
     *
     * @throws IllegalStateException if the endpoint has neither a listener nor a handler method
     */
    @Override
    public SolaceMessageListenerContainer createListenerContainer(SolaceListenerEndpoint endpoint) {
        SolaceMessageListener listener = endpoint.getMessageListener();
        if (listener == null && !endpoint.getDispatchTargets().isEmpty()) {
            listener = topicDispatchingListener(endpoint);
        }
        if (listener == null) {
            Assert.state(endpoint.getInvocableHandlerMethod() != null,
                    "Endpoint '" + endpoint.getId() + "' has neither a message listener nor a handler method");
            listener = adapterFor(endpoint.getInvocableHandlerMethod(), endpoint.getPayloadType(),
                    endpoint.getReplyDestination());
        }
        else if (listener instanceof AbstractSolaceListenerAdapter adapter) {
            if (adapter.replyTemplate == null) {
                adapter.setReplyTemplate(this.replyTemplate);
            }
        }
        DefaultSolaceMessageListenerContainer container = new DefaultSolaceMessageListenerContainer(
                this.sessionFactory, endpoint, this.containerProperties,
                this.instanceIdProvider.getInstanceId());
        container.setupMessageListener(listener);
        container.setTransactionManager(this.transactionManager);
        container.setTaskExecutor(this.taskExecutor);
        container.setListenerMetrics(this.listenerMetrics);
        container.setFlowListener(this.flowListener);
        if (this.errorHandler != null) {
            container.setErrorHandler(this.errorHandler);
        }
        return container;
    }

    /**
     * Build the routing table for a topic-dispatching endpoint.
     *
     * <p>Each target gets its own adapter, and therefore its own payload type &mdash; which is the
     * whole point of sharing an endpoint between methods that take different types.</p>
     *
     * @param endpoint the endpoint carrying the dispatch targets
     * @return the dispatching listener
     */
    private SolaceMessageListener topicDispatchingListener(SolaceListenerEndpoint endpoint) {
        List<TopicDispatchingSolaceListener.Target> targets = endpoint.getDispatchTargets().stream()
                .map(target -> new TopicDispatchingSolaceListener.Target(
                        List.copyOf(target.getTopics()),
                        adapterFor(target.getInvocableHandlerMethod(), target.getPayloadType(),
                                target.getReplyDestination()),
                        target.getDescription()))
                .toList();
        return new TopicDispatchingSolaceListener(endpoint.getId(), targets);
    }

    /**
     * Build the adapter for one handler method.
     *
     * @param handlerMethod    the method to invoke
     * @param payloadType      the type its message bodies are converted into
     * @param replyDestination overrides where its return value is published, or empty
     * @return the adapter
     */
    private MethodSolaceListenerAdapter adapterFor(
            org.springframework.messaging.handler.invocation.InvocableHandlerMethod handlerMethod,
            Class<?> payloadType, String replyDestination) {
        MethodSolaceListenerAdapter adapter = new MethodSolaceListenerAdapter(handlerMethod,
                this.messageConverter, this.headerMapper);
        adapter.setPayloadType(payloadType);
        adapter.setReplyDestination(replyDestination);
        adapter.setReplyTemplate(this.replyTemplate);
        return adapter;
    }

    /**
     * The defaults every container from this factory starts with.
     *
     * @return the shared defaults, mutable so that a configurer can adjust them before any container
     *         is created
     */
    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }
}
