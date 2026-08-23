package cris.prs.messaging.solace.listener;

import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.core.ExchangePattern;
import cris.prs.messaging.solace.core.SettlementOutcome;
import lombok.Data;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Everything the container factory needs to build a listener container: the Solace analogue of
 * Spring for Apache Kafka's {@code KafkaListenerEndpoint}.
 */
@Data
public class SolaceListenerEndpoint {

    /** Create an empty endpoint description; every unset field falls back to a container default. */
    public SolaceListenerEndpoint() {
    }


    /** Container id; defaults to a generated value when not set on {@code @SolaceListener}. */
    private String id;

    /** Topic subscriptions added to the endpoint (or to the session in {@code DIRECT} mode). */
    private List<String> topics = new ArrayList<>();

    /** Queue (endpoint) name; ignored in {@code DIRECT} mode. */
    private String queue;

    /** Consumer group, appended to the queue name as {@code <queue>.<group>}. */
    private String group;

    /** The exchange pattern this listener realises; {@code null} means "no preset applied". */
    private ExchangePattern pattern;

    /** {@code null} means "use the container factory default". */
    private EndpointMode endpointMode;

    /** Endpoint access type; {@code null} means "use the container factory default". */
    private ContainerProperties.AccessType accessType;

    /** Broker side selector evaluated against the message's SDT user properties. */
    private String selector;

    private Integer concurrency;

    private Boolean transactional;

    /** Overrides {@code solace.listener.error-outcome} for this listener; unset inherits it. */
    private SettlementOutcome errorOutcome;

    /** {@code null} means "use the container factory default". */
    private ContainerProperties.DispatchMode dispatch;

    private Boolean autoStartup;

    /** Append the instance id to the queue name, giving every pod its own endpoint. */
    private Boolean appendInstanceIdToQueue;

    /** Append the instance id as an extra topic level to every subscription. */
    private Boolean appendInstanceIdToTopics;

    /** Static reply destination; when unset, replies go to the request's {@code replyTo}. */
    private String replyDestination;

    /** Type the message body is converted into before the listener is invoked. */
    private Class<?> payloadType = Object.class;

    /** Set for {@code @SolaceListener} methods. */
    private InvocableHandlerMethod invocableHandlerMethod;

    /** Set for programmatically registered listeners. */
    private SolaceMessageListener messageListener;

    /**
     * Fill in the endpoint wiring implied by {@link #pattern}, leaving anything already set alone.
     *
     * <p>Fan-out and competing consumers differ only in whether each instance binds its own endpoint
     * or they all share one, so the pattern is expressed here rather than left to three flags the
     * caller has to keep consistent.</p>
     */
    public void applyPatternDefaults() {
        if (this.pattern == null) {
            return;
        }
        switch (this.pattern) {
            case PUBLISH_SUBSCRIBE -> {
                // Its own endpoint per instance, so every instance receives every message.
                if (this.endpointMode == null) {
                    this.endpointMode = EndpointMode.NON_DURABLE_QUEUE;
                }
                if (this.appendInstanceIdToQueue == null) {
                    this.appendInstanceIdToQueue = true;
                }
                if (this.accessType == null) {
                    this.accessType = ContainerProperties.AccessType.EXCLUSIVE;
                }
                if (this.concurrency == null) {
                    // An exclusive endpoint admits a single consumer, so extra flows cannot process
                    // anything; the broker rejects the surplus binds outright on a temporary queue
                    // ("503 Max clients exceeded for queue"). Parallelism in fan-out comes from
                    // running more instances, which is the point of the pattern.
                    this.concurrency = 1;
                }
            }
            case POINT_TO_POINT -> {
                // One shared durable endpoint all instances compete over.
                if (this.endpointMode == null) {
                    this.endpointMode = EndpointMode.DURABLE_QUEUE;
                }
                if (this.appendInstanceIdToQueue == null) {
                    this.appendInstanceIdToQueue = false;
                }
                if (this.accessType == null) {
                    this.accessType = ContainerProperties.AccessType.NONEXCLUSIVE;
                }
            }
            case REQUEST_REPLY -> {
                // A shared request endpoint; the reply goes to the requester's own destination.
                if (this.appendInstanceIdToQueue == null) {
                    this.appendInstanceIdToQueue = false;
                }
                if (this.accessType == null) {
                    this.accessType = ContainerProperties.AccessType.NONEXCLUSIVE;
                }
            }
        }
    }

    /**
     * Resolve the physical endpoint name.
     *
     * <p>{@code <queue>[.<group>][.<instanceId>]}, falling back to the container id when no queue is
     * set. Whether the instance id is appended is the difference between fan-out and competing
     * consumers.</p>
     *
     * @param instanceId this instance's id; ignored unless {@code appendInstanceIdToQueue} is set
     * @return the endpoint name to bind
     */
    public String resolveQueueName(String instanceId) {
        StringBuilder name = new StringBuilder(StringUtils.hasText(this.queue) ? this.queue : this.id);
        if (StringUtils.hasText(this.group)) {
            name.append('.').append(this.group);
        }
        if (Boolean.TRUE.equals(this.appendInstanceIdToQueue) && StringUtils.hasText(instanceId)) {
            name.append('.').append(instanceId);
        }
        return name.toString();
    }

    /**
     * Resolve the topic subscriptions.
     *
     * @param instanceId this instance's id; appended as an extra topic level only when
     *                   {@code appendInstanceIdToTopics} is set
     * @return the subscriptions to add to the endpoint
     */
    public List<String> resolveTopics(String instanceId) {
        if (!Boolean.TRUE.equals(this.appendInstanceIdToTopics) || !StringUtils.hasText(instanceId)) {
            return this.topics;
        }
        List<String> resolved = new ArrayList<>(this.topics.size());
        for (String topic : this.topics) {
            resolved.add(topic + "/" + instanceId);
        }
        return resolved;
    }
}
