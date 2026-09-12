package org.cris.prs.solace.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.cris.prs.messaging.solace.core.DefaultSolaceHeaderMapper;
import org.cris.prs.messaging.solace.core.DefaultSolaceSessionFactory;
import org.cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceHeaderMapper;
import org.cris.prs.messaging.solace.core.SolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.cris.prs.messaging.solace.core.SolaceSessionListener;
import org.cris.prs.messaging.solace.core.SolaceTemplate;
import org.cris.prs.messaging.solace.listener.DefaultSolaceListenerContainerFactory;
import org.cris.prs.messaging.solace.listener.SolaceFlowListener;
import org.cris.prs.messaging.solace.listener.SolaceListenerConfigUtils;
import org.cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.cris.prs.messaging.solace.listener.SolaceListenerMetrics;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplateFactory;
import org.cris.prs.messaging.solace.requestreply.SolaceRequestReplyMetrics;
import org.cris.prs.messaging.solace.support.HostnameInstanceIdProvider;
import org.cris.prs.messaging.solace.support.InstanceIdProvider;
import org.cris.prs.messaging.solace.transaction.SolaceTransactionManager;
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


/**
 * Auto-configuration for Solace messaging: session factory, {@code SolaceTemplate},
 * {@code ReplyingSolaceTemplate}, listener container factory, transaction manager and
 * {@code @SolaceListener} support.
 *
 * <p>It is the Solace equivalent of Spring Boot's {@code KafkaAutoConfiguration}, and like that one
 * it turns on annotation driven listeners automatically, so applications do not have to declare
 * {@link org.cris.prs.messaging.solace.annotation.EnableSolace} themselves.</p>
 *
 * <p>This class deliberately lives outside the application's component scanned packages: an
 * auto-configuration class that is component scanned has its conditions evaluated too early, before
 * the Solace starter has contributed {@code SpringJCSMPFactory}, and every bean below would be
 * silently skipped.</p>
 */
@AutoConfiguration(afterName = {
        "com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration",
        // So that a MeterRegistry, if the application has one, exists before the optional
        // observability configuration decides whether to instrument anything.
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"})
@ConditionalOnClass({JCSMPSession.class, SpringJCSMPFactory.class})
@EnableConfigurationProperties(SolaceProperties.class)
// SolaceSchemaRegistryConfiguration comes first: an imported configuration's beans are registered
// before this class's own, which is what lets its converter make solaceMessageConverter() back off.
@Import({SolaceSchemaRegistryConfiguration.class, SolaceAnnotationDrivenConfiguration.class,
        SolaceObservabilityConfiguration.class})
public class SolaceAutoConfiguration {

    /** Create the auto-configuration. Instantiated by Spring Boot, not by application code. */
    public SolaceAutoConfiguration() {
    }


    /**
     * Identifies this application instance, seeding every per-instance destination name.
     *
     * @param properties supplies {@code solace.instance-id} when it is set
     * @return a provider resolving the id from the override, {@code $HOSTNAME}, {@code $POD_NAME} or
     *         the local host name
     */
    @Bean
    @ConditionalOnMissingBean
    public InstanceIdProvider solaceInstanceIdProvider(SolaceProperties properties) {
        return new HostnameInstanceIdProvider(properties.getInstanceId());
    }

    /**
     * JSON payload conversion.
     *
     * @param objectMapper the application's mapper when one exists, so that modules and naming
     *                     strategies match the rest of the application; a default mapper otherwise
     * @return the converter used by every template and listener
     */
    @Bean
    @ConditionalOnMissingBean
    public SolaceMessageConverter solaceMessageConverter(ObjectProvider<ObjectMapper> objectMapper) {
        ObjectMapper mapper = objectMapper.getIfAvailable(ObjectMapper::new);
        return new JacksonSolaceMessageConverter(mapper);
    }

    /**
     * Header mapping between Spring headers and Solace message fields.
     *
     * @return the mapper translating between Spring headers, native Solace fields and SDT user
     *         properties
     */
    @Bean
    @ConditionalOnMissingBean
    public SolaceHeaderMapper solaceHeaderMapper() {
        return new DefaultSolaceHeaderMapper();
    }

    /**
     * The connection factory every other bean here builds on.
     *
     * @param springJCSMPFactory contributed by {@code solace-java-spring-boot-starter} from
     *                           {@code solace.java.*}
     * @param sessionListener    optional session lifecycle callback; without one the factory's own
     *                           logging is the only reporting of a reconnect
     * @return the session factory, which is also the key Solace transactions bind under
     */
    @Bean
    @ConditionalOnMissingBean
    public SolaceSessionFactory solaceSessionFactory(SpringJCSMPFactory springJCSMPFactory,
            ObjectProvider<SolaceSessionListener> sessionListener) {
        DefaultSolaceSessionFactory factory = new DefaultSolaceSessionFactory(springJCSMPFactory);
        factory.setSessionListener(sessionListener.getIfAvailable());
        return factory;
    }

    /**
     * Enables {@code @Transactional} and {@code TransactionTemplate} over Solace local transactions.
     *
     * @param sessionFactory supplies transacted sessions
     * @return the transaction manager
     */
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
    /**
     * The general purpose template, configured from {@code solace.template.*}.
     *
     * <p>Marked primary because {@code ReplyingSolaceTemplate} extends {@code SolaceTemplate}, so both
     * beans match an unqualified {@code SolaceTemplate} injection point once request-reply is enabled.
     * Application code asking for a plain template wants this one; asking for the request-reply
     * behaviour means injecting {@code ReplyingSolaceTemplate} by its own type.</p>
     *
     * @param sessionFactory   supplies the connection
     * @param messageConverter serialises payloads
     * @param headerMapper     applies headers
     * @param properties       supplies the delivery mode, expiry, priority, DMQ eligibility and
     *                         default destination
     * @return the primary template
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
    /**
     * Executor backing {@code dispatch: EXECUTOR} listener containers.
     *
     * <p>Each flow submits a single long-lived invoker task, so this creates exactly one thread per
     * flow &mdash; the arrangement Spring's {@code DefaultMessageListenerContainer} uses for JMS.</p>
     *
     * <p>The threads are non-daemon, which is what lets a consumer-only application stay alive without
     * {@code solace.listener.keep-alive}. Replacing this bean with a virtual thread executor is fine,
     * but virtual threads are daemon threads, so keep-alive must then stay on.</p>
     *
     * @return the executor listener invokers run on
     */
    @Bean(name = "solaceListenerTaskExecutor")
    @ConditionalOnMissingBean(name = "solaceListenerTaskExecutor")
    public AsyncTaskExecutor solaceListenerTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("solace-listener-");
        executor.setDaemon(false);
        return executor;
    }

    /**
     * The default container factory, used by every {@code @SolaceListener} that does not name another.
     *
     * @param sessionFactory       supplies connections
     * @param messageConverter     converts message bodies
     * @param headerMapper         maps headers
     * @param instanceIdProvider   supplies per-instance endpoint names
     * @param properties           supplies the container defaults from {@code solace.listener.*}
     * @param transactionManager   drives transactional containers
     * @param solaceTemplate       publishes listener return values as replies; qualified explicitly so
     *                             replies never go through the template tracking outstanding requests
     * @param listenerTaskExecutor runs invokers under {@code EXECUTOR} dispatch
     * @param listenerMetrics      optional instrumentation; every container falls back to the no-op
     *                             collaborator when Micrometer is absent or metrics are disabled
     * @param flowListener         optional flow lifecycle callback; without one the container's own
     *                             logging is the only reporting
     * @param errorHandler         optional; decides what happens to a message whose listener threw.
     *                             Without one each container logs the failure and applies its
     *                             configured {@code errorOutcome}
     * @return the container factory
     */
    @Bean(name = SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    @ConditionalOnMissingBean(name = SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    public DefaultSolaceListenerContainerFactory solaceListenerContainerFactory(
            SolaceSessionFactory sessionFactory, SolaceMessageConverter messageConverter,
            SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIdProvider,
            SolaceProperties properties, SolaceTransactionManager transactionManager,
            @Qualifier("solaceTemplate") SolaceTemplate<Object> solaceTemplate,
            @Qualifier("solaceListenerTaskExecutor") AsyncTaskExecutor listenerTaskExecutor,
            ObjectProvider<SolaceListenerMetrics> listenerMetrics,
            ObjectProvider<SolaceFlowListener> flowListener,
            ObjectProvider<SolaceListenerErrorHandler> errorHandler) {
        DefaultSolaceListenerContainerFactory factory = new DefaultSolaceListenerContainerFactory(
                sessionFactory, messageConverter, headerMapper, instanceIdProvider, properties.getListener());
        factory.setTransactionManager(transactionManager);
        factory.setReplyTemplate(solaceTemplate);
        factory.setTaskExecutor(listenerTaskExecutor);
        factory.setListenerMetrics(listenerMetrics.getIfAvailable(() -> SolaceListenerMetrics.NO_OP));
        factory.setFlowListener(flowListener.getIfAvailable());
        errorHandler.ifAvailable(factory::setErrorHandler);
        return factory;
    }

    /**
     * Builds request-reply templates and the containers that consume their reply destinations.
     *
     * <p>Exposed as a bean so that an application can declare <em>additional</em> reply destinations
     * &mdash; one per service, where a shared one is not appropriate. See {@code ReplyEndpointSpec}
     * for when that is worth the extra endpoint.</p>
     *
     * @param sessionFactory     supplies the connection
     * @param messageConverter   converts requests and replies
     * @param headerMapper       applies headers
     * @param instanceIdProvider supplies the id that makes each instance's reply destination unique
     * @param properties         supplies the publishing defaults ({@code solace.template.*}) applied to
     *                           every template this factory creates whose spec leaves them unset
     * @param metrics            optional instrumentation; every template falls back to the no-op
     *                           collaborator when Micrometer is absent or metrics are disabled
     * @return the factory
     */
    @Bean
    @ConditionalOnMissingBean
    public ReplyingSolaceTemplateFactory replyingSolaceTemplateFactory(SolaceSessionFactory sessionFactory,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper,
            InstanceIdProvider instanceIdProvider, SolaceProperties properties,
            ObjectProvider<SolaceRequestReplyMetrics> metrics) {
        ReplyingSolaceTemplateFactory factory = new ReplyingSolaceTemplateFactory(sessionFactory,
                messageConverter, headerMapper, instanceIdProvider);
        factory.setRequestReplyMetrics(metrics.getIfAvailable(() -> SolaceRequestReplyMetrics.NO_OP));
        // Publishing defaults, so a reply template an application declares publishes like solaceTemplate
        // unless its spec says otherwise. A spec that states one of these wins over the default.
        SolaceProperties.Template templateProperties = properties.getTemplate();
        factory.setDefaultTimeToLive(templateProperties.getTimeToLive());
        factory.setDefaultPriority(templateProperties.getPriority());
        factory.setDefaultDmqEligible(templateProperties.isDmqEligible());
        return factory;
    }

    /**
     * The application's default request-reply template, consuming this instance's own reply
     * destination.
     *
     * <p>One reply destination is shared by every service the application calls: the reply channel
     * belongs to the requester, and the correlation id returns each reply to its request. Declare a
     * further bean from {@link #replyingSolaceTemplateFactory} to give a particular service its own;
     * because the condition below is matched by <em>name</em>, declaring such a bean adds to this one
     * rather than replacing it. To replace it, declare a bean named {@code replyingSolaceTemplate}.</p>
     *
     * @param factory    builds the template and its reply container
     * @param properties supplies the reply destination, timeout and delivery mode
     * @return the request-reply template
     */
    @Bean
    @ConditionalOnMissingBean(name = "replyingSolaceTemplate")
    @ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    public ReplyingSolaceTemplate replyingSolaceTemplate(ReplyingSolaceTemplateFactory factory,
            SolaceProperties properties) {
        return factory.create(properties.getRequestReply());
    }
}
