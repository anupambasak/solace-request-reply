package cris.prs.messaging.solace.listener;

import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.core.SolaceTemplate;
import cris.prs.messaging.solace.support.InstanceIdProvider;
import lombok.Setter;
import org.springframework.util.Assert;

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
        if (listener == null) {
            Assert.state(endpoint.getInvocableHandlerMethod() != null,
                    "Endpoint '" + endpoint.getId() + "' has neither a message listener nor a handler method");
            MethodSolaceListenerAdapter adapter = new MethodSolaceListenerAdapter(
                    endpoint.getInvocableHandlerMethod(), this.messageConverter, this.headerMapper);
            adapter.setPayloadType(endpoint.getPayloadType());
            adapter.setReplyDestination(endpoint.getReplyDestination());
            adapter.setReplyTemplate(this.replyTemplate);
            listener = adapter;
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
     * The defaults every container from this factory starts with.
     *
     * @return the shared defaults, mutable so that a configurer can adjust them before any container
     *         is created
     */
    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }
}
