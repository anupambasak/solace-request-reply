package cris.prs.solace.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import cris.prs.messaging.solace.core.DefaultSolaceHeaderMapper;
import cris.prs.messaging.solace.core.DefaultSolaceSessionFactory;
import cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.core.SolaceTemplate;
import cris.prs.messaging.solace.listener.ContainerProperties;
import cris.prs.messaging.solace.listener.DefaultSolaceListenerContainerFactory;
import cris.prs.messaging.solace.listener.DefaultSolaceMessageListenerContainer;
import cris.prs.messaging.solace.listener.SolaceListenerConfigUtils;
import cris.prs.messaging.solace.listener.SolaceListenerEndpoint;
import cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.support.HostnameInstanceIdProvider;
import cris.prs.messaging.solace.support.InstanceIdProvider;
import cris.prs.messaging.solace.support.ReplyDestinationResolver;
import cris.prs.messaging.solace.transaction.SolaceTransactionManager;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;

/**
 * Auto-configuration for Solace messaging: session factory, {@code SolaceTemplate},
 * {@code ReplyingSolaceTemplate}, listener container factory, transaction manager and
 * {@code @SolaceListener} support.
 *
 * <p>It is the Solace equivalent of Spring Boot's {@code KafkaAutoConfiguration}, and like that one
 * it turns on annotation driven listeners automatically, so applications do not have to declare
 * {@link cris.prs.messaging.solace.annotation.EnableSolace} themselves.</p>
 *
 * <p>This class deliberately lives outside the application's component scanned packages: an
 * auto-configuration class that is component scanned has its conditions evaluated too early, before
 * the Solace starter has contributed {@code SpringJCSMPFactory}, and every bean below would be
 * silently skipped.</p>
 */
@AutoConfiguration(afterName = "com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration")
@ConditionalOnClass({JCSMPSession.class, SpringJCSMPFactory.class})
@EnableConfigurationProperties(SolaceProperties.class)
@Import(SolaceAnnotationDrivenConfiguration.class)
public class SolaceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public InstanceIdProvider solaceInstanceIdProvider(SolaceProperties properties) {
        return new HostnameInstanceIdProvider(properties.getInstanceId());
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceMessageConverter solaceMessageConverter(ObjectProvider<ObjectMapper> objectMapper) {
        ObjectMapper mapper = objectMapper.getIfAvailable(ObjectMapper::new);
        return new JacksonSolaceMessageConverter(mapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceHeaderMapper solaceHeaderMapper() {
        return new DefaultSolaceHeaderMapper();
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceSessionFactory solaceSessionFactory(SpringJCSMPFactory springJCSMPFactory) {
        return new DefaultSolaceSessionFactory(springJCSMPFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceTransactionManager solaceTransactionManager(SolaceSessionFactory sessionFactory) {
        return new SolaceTransactionManager(sessionFactory);
    }

    /**
     * The general purpose template.
     *
     * <p>Marked primary because {@code ReplyingSolaceTemplate} extends {@code SolaceTemplate}, so
     * both beans match an unqualified {@code SolaceTemplate} injection point once request-reply is
     * enabled. Application code that asks for a plain template wants this one; asking for the
     * request-reply behaviour means injecting {@code ReplyingSolaceTemplate} by its own type.</p>
     */
    @Bean
    @Primary
    @ConditionalOnMissingBean(name = "solaceTemplate")
    public SolaceTemplate<Object> solaceTemplate(SolaceSessionFactory sessionFactory,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper,
            SolaceProperties properties) {
        SolaceTemplate<Object> template = new SolaceTemplate<>(sessionFactory, messageConverter);
        template.setHeaderMapper(headerMapper);
        SolaceProperties.Template templateProperties = properties.getTemplate();
        template.setDefaultDestination(templateProperties.getDefaultDestination());
        template.setDeliveryMode(templateProperties.getDeliveryMode());
        template.setTimeToLive(templateProperties.getTimeToLive());
        template.setPriority(templateProperties.getPriority());
        template.setDmqEligible(templateProperties.isDmqEligible());
        return template;
    }

    /**
     * Executor backing {@code dispatch: EXECUTOR} listener containers. Each flow submits a single
     * long-lived invoker task, so this creates exactly one thread per flow, the same arrangement
     * Spring's {@code DefaultMessageListenerContainer} uses for JMS.
     *
     * <p>The threads are non-daemon, which is what lets a consumer-only application stay alive
     * without {@code solace.listener.keep-alive}. Replacing this bean with a virtual thread
     * executor is fine, but virtual threads are daemon threads, so keep-alive must stay on.</p>
     */
    @Bean(name = "solaceListenerTaskExecutor")
    @ConditionalOnMissingBean(name = "solaceListenerTaskExecutor")
    public AsyncTaskExecutor solaceListenerTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("solace-listener-");
        executor.setDaemon(false);
        return executor;
    }

    @Bean(name = SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    @ConditionalOnMissingBean(name = SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    public DefaultSolaceListenerContainerFactory solaceListenerContainerFactory(
            SolaceSessionFactory sessionFactory, SolaceMessageConverter messageConverter,
            SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIdProvider,
            SolaceProperties properties, SolaceTransactionManager transactionManager,
            @Qualifier("solaceTemplate") SolaceTemplate<Object> solaceTemplate,
            @Qualifier("solaceListenerTaskExecutor") AsyncTaskExecutor listenerTaskExecutor) {
        DefaultSolaceListenerContainerFactory factory = new DefaultSolaceListenerContainerFactory(
                sessionFactory, messageConverter, headerMapper, instanceIdProvider, properties.getListener());
        factory.setTransactionManager(transactionManager);
        factory.setReplyTemplate(solaceTemplate);
        factory.setTaskExecutor(listenerTaskExecutor);
        return factory;
    }

    /**
     * The per-instance reply container. Its endpoint carries the instance id, both in the topic
     * subscription and, for queue based modes, in the endpoint name.
     */
    @Bean(name = "solaceReplyContainer")
    @ConditionalOnMissingBean(name = "solaceReplyContainer")
    @ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    public SolaceMessageListenerContainer solaceReplyContainer(SolaceSessionFactory sessionFactory,
            SolaceProperties properties, InstanceIdProvider instanceIdProvider) {
        SolaceProperties.RequestReply requestReply = properties.getRequestReply();
        String instanceId = instanceIdProvider.getInstanceId();

        SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
        endpoint.setId("solaceReplyContainer");
        endpoint.setTopics(List.of(ReplyDestinationResolver.resolveTopic(
                requestReply.getReplyTopicPrefix(), requestReply.isAppendInstanceId(), instanceId)));
        endpoint.setQueue(ReplyDestinationResolver.resolveQueueBaseName(
                requestReply.getReplyQueue(), requestReply.getReplyTopicPrefix()));
        endpoint.setGroup(requestReply.getReplyGroup());
        endpoint.setAppendInstanceIdToQueue(requestReply.isAppendInstanceId());
        endpoint.setEndpointMode(requestReply.getEndpointMode());
        endpoint.setConcurrency(requestReply.getConcurrency());
        endpoint.setSelector(requestReply.getSelector());
        // Replies are correlated in memory; consuming them transactionally would only add latency.
        endpoint.setTransactional(false);
        // Started by the ReplyingSolaceTemplate so that no reply can arrive before it is ready.
        endpoint.setAutoStartup(false);

        ContainerProperties containerProperties = new ContainerProperties();
        containerProperties.setEndpointMode(requestReply.getEndpointMode());
        containerProperties.setConcurrency(requestReply.getConcurrency());
        containerProperties.setAutoStartup(false);
        // The requesting application drives its own lifetime; a reply container should not keep the
        // JVM alive by itself the way a server side listener does.
        containerProperties.setKeepAlive(false);
        containerProperties.getEndpoint()
                .setAccessType(requestReply.getConcurrency() > 1
                        ? ContainerProperties.AccessType.NONEXCLUSIVE
                        : ContainerProperties.AccessType.EXCLUSIVE);

        return new DefaultSolaceMessageListenerContainer(sessionFactory, endpoint, containerProperties,
                instanceId);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    public ReplyingSolaceTemplate replyingSolaceTemplate(SolaceSessionFactory sessionFactory,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper,
            SolaceProperties properties, InstanceIdProvider instanceIdProvider,
            SolaceMessageListenerContainer solaceReplyContainer) {
        SolaceProperties.RequestReply requestReply = properties.getRequestReply();
        String replyDestination = ReplyDestinationResolver.resolveTopic(requestReply.getReplyTopicPrefix(),
                requestReply.isAppendInstanceId(), instanceIdProvider.getInstanceId());

        ReplyingSolaceTemplate template = new ReplyingSolaceTemplate(sessionFactory, messageConverter,
                solaceReplyContainer, replyDestination);
        template.setHeaderMapper(headerMapper);
        template.setDeliveryMode(requestReply.getDeliveryMode());
        template.setDefaultReplyTimeout(requestReply.getReplyTimeout());
        template.setInstanceId(instanceIdProvider.getInstanceId());
        template.setDmqEligible(properties.getTemplate().isDmqEligible());
        template.setTimeToLive(properties.getTemplate().getTimeToLive());
        return template;
    }
}
