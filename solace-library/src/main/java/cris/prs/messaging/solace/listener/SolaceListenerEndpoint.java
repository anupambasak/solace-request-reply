package cris.prs.messaging.solace.listener;

import cris.prs.messaging.solace.core.EndpointMode;
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

    /** Container id; defaults to a generated value when not set on {@code @SolaceListener}. */
    private String id;

    /** Topic subscriptions added to the endpoint (or to the session in {@code DIRECT} mode). */
    private List<String> topics = new ArrayList<>();

    /** Queue (endpoint) name; ignored in {@code DIRECT} mode. */
    private String queue;

    /** Consumer group, appended to the queue name as {@code <queue>.<group>}. */
    private String group;

    /** {@code null} means "use the container factory default". */
    private EndpointMode endpointMode;

    /** Broker side selector evaluated against the message's SDT user properties. */
    private String selector;

    private Integer concurrency;

    private Boolean transactional;

    /** {@code null} means "use the container factory default". */
    private ContainerProperties.DispatchMode dispatch;

    private Boolean autoStartup;

    /** Append the instance id to the queue name, giving every pod its own endpoint. */
    private boolean appendInstanceIdToQueue;

    /** Append the instance id as an extra topic level to every subscription. */
    private boolean appendInstanceIdToTopics;

    /** Static reply destination; when unset, replies go to the request's {@code replyTo}. */
    private String replyDestination;

    /** Type the message body is converted into before the listener is invoked. */
    private Class<?> payloadType = Object.class;

    /** Set for {@code @SolaceListener} methods. */
    private InvocableHandlerMethod invocableHandlerMethod;

    /** Set for programmatically registered listeners. */
    private SolaceMessageListener messageListener;

    /**
     * Resolve the physical endpoint name from the queue, the consumer group and, when requested,
     * the instance id.
     */
    public String resolveQueueName(String instanceId) {
        StringBuilder name = new StringBuilder(StringUtils.hasText(this.queue) ? this.queue : this.id);
        if (StringUtils.hasText(this.group)) {
            name.append('.').append(this.group);
        }
        if (this.appendInstanceIdToQueue && StringUtils.hasText(instanceId)) {
            name.append('.').append(instanceId);
        }
        return name.toString();
    }

    /** Resolve the topic subscriptions, appending the instance id level when requested. */
    public List<String> resolveTopics(String instanceId) {
        if (!this.appendInstanceIdToTopics || !StringUtils.hasText(instanceId)) {
            return this.topics;
        }
        List<String> resolved = new ArrayList<>(this.topics.size());
        for (String topic : this.topics) {
            resolved.add(topic + "/" + instanceId);
        }
        return resolved;
    }
}
