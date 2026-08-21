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
        if (this.errorHandler != null) {
            container.setErrorHandler(this.errorHandler);
        }
        return container;
    }

    public ContainerProperties getContainerProperties() {
        return this.containerProperties;
    }
}
