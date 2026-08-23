package cris.prs.messaging.solace.requestreply;

import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.listener.ContainerProperties;
import cris.prs.messaging.solace.listener.DefaultSolaceMessageListenerContainer;
import cris.prs.messaging.solace.listener.SolaceListenerEndpoint;
import cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import cris.prs.messaging.solace.support.InstanceIdProvider;
import cris.prs.messaging.solace.support.ReplyDestinationResolver;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Builds a {@link ReplyingSolaceTemplate} together with the container that consumes its reply
 * destination.
 *
 * <p>The auto-configuration uses this to create the application's default request-reply template.
 * Declare a second bean from the same factory to give a service its own reply destination &mdash;
 * see {@link ReplyEndpointSpec} for when that is worth the extra endpoint:</p>
 *
 * <pre>{@code
 * @Bean
 * ReplyingSolaceTemplate inventoryReplyingSolaceTemplate(ReplyingSolaceTemplateFactory factory) {
 *     ReplyEndpointSpec spec = new ReplyEndpointSpec();
 *     spec.setId("inventoryReplyContainer");
 *     spec.setReplyTopicPrefix("app/reply/inventory");
 *     return factory.create(spec);
 * }
 * }</pre>
 *
 * <p>The returned template is a {@code SmartLifecycle}, so Spring starts and stops it &mdash; and it
 * owns its container, starting it only once the correlation map is live.</p>
 */
public class ReplyingSolaceTemplateFactory {

    private final SolaceSessionFactory sessionFactory;

    private final SolaceMessageConverter messageConverter;

    private final SolaceHeaderMapper headerMapper;

    private final InstanceIdProvider instanceIdProvider;

    /**
     * Create a factory.
     *
     * @param sessionFactory     supplies the connection and keys transactions
     * @param messageConverter   converts request payloads and reply bodies
     * @param headerMapper       applies headers to requests
     * @param instanceIdProvider supplies the id that makes each instance's reply destination unique
     */
    public ReplyingSolaceTemplateFactory(SolaceSessionFactory sessionFactory,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper,
            InstanceIdProvider instanceIdProvider) {
        Assert.notNull(sessionFactory, "'sessionFactory' must not be null");
        this.sessionFactory = sessionFactory;
        this.messageConverter = messageConverter;
        this.headerMapper = headerMapper;
        this.instanceIdProvider = instanceIdProvider;
    }

    /**
     * Build a request-reply template and its reply container.
     *
     * @param spec the reply destination to consume and the template defaults to apply
     * @return a template, not yet started; Spring starts it as a {@code SmartLifecycle} bean
     */
    public ReplyingSolaceTemplate create(ReplyEndpointSpec spec) {
        Assert.notNull(spec, "'spec' must not be null");
        String instanceId = this.instanceIdProvider.getInstanceId();
        String replyDestination = ReplyDestinationResolver.resolveTopic(
                spec.getReplyTopicPrefix(), spec.isAppendInstanceId(), instanceId);

        ReplyingSolaceTemplate template = new ReplyingSolaceTemplate(this.sessionFactory,
                this.messageConverter, createReplyContainer(spec, instanceId), replyDestination);
        template.setHeaderMapper(this.headerMapper);
        template.setDeliveryMode(spec.getDeliveryMode());
        template.setDefaultReplyTimeout(spec.getReplyTimeout());
        template.setInstanceId(instanceId);
        return template;
    }

    /**
     * Build the container consuming one reply destination.
     *
     * <p>Configured deliberately: not auto-started, because the template starts it once the
     * correlation map is live; not keep-alive, because a requesting application decides its own
     * lifetime; and exclusive only while a single flow consumes it.</p>
     *
     * @param spec       the reply destination to consume
     * @param instanceId this instance's id
     * @return the reply container, not started
     */
    protected SolaceMessageListenerContainer createReplyContainer(ReplyEndpointSpec spec, String instanceId) {
        SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
        endpoint.setId(spec.getId());
        endpoint.setTopics(List.of(ReplyDestinationResolver.resolveTopic(
                spec.getReplyTopicPrefix(), spec.isAppendInstanceId(), instanceId)));
        endpoint.setQueue(ReplyDestinationResolver.resolveQueueBaseName(
                spec.getReplyQueue(), spec.getReplyTopicPrefix()));
        endpoint.setGroup(spec.getReplyGroup());
        endpoint.setAppendInstanceIdToQueue(spec.isAppendInstanceId());
        endpoint.setEndpointMode(spec.getEndpointMode());
        endpoint.setConcurrency(spec.getConcurrency());
        endpoint.setSelector(spec.getSelector());
        // Replies are correlated in memory; consuming them transactionally would only add latency.
        endpoint.setTransactional(false);
        endpoint.setAutoStartup(false);

        ContainerProperties containerProperties = new ContainerProperties();
        containerProperties.setEndpointMode(spec.getEndpointMode());
        containerProperties.setConcurrency(spec.getConcurrency());
        containerProperties.setAutoStartup(false);
        containerProperties.setKeepAlive(false);
        containerProperties.getEndpoint()
                .setAccessType(spec.getConcurrency() > 1
                        ? ContainerProperties.AccessType.NONEXCLUSIVE
                        : ContainerProperties.AccessType.EXCLUSIVE);

        return new DefaultSolaceMessageListenerContainer(this.sessionFactory, endpoint,
                containerProperties, instanceId);
    }
}
