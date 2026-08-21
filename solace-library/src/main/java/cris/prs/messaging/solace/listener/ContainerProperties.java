package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.EndpointProperties;
import cris.prs.messaging.solace.core.EndpointMode;
import lombok.Data;

import java.time.Duration;

/** Tuning knobs for a listener container, analogous to Kafka's {@code ContainerProperties}. */
@Data
public class ContainerProperties {

    /** How the consumer binds to the broker: durable queue, temporary queue or direct topic. */
    private EndpointMode endpointMode = EndpointMode.DURABLE_QUEUE;

    /** Number of consumer flows created for the endpoint. */
    private int concurrency = 1;

    /**
     * Consume inside a Solace local transaction. The container creates one transacted session per
     * flow, so the request acknowledgement and any reply published by the listener commit together.
     */
    private boolean transactional = false;

    private boolean autoStartup = true;

    /** Provision the durable endpoint at startup if it does not already exist. */
    private boolean provisionEndpoint = true;

    /**
     * Acknowledge a message whose listener threw. When {@code false} the message stays unacknowledged
     * and is redelivered after the flow is rebound. Ignored for transactional containers, which roll
     * the transaction back instead.
     */
    private boolean ackOnError = true;

    /**
     * Where the listener is invoked: on the JCSMP delivery thread (INLINE) or on a Spring
     * {@code AsyncTaskExecutor} (EXECUTOR).
     */
    private DispatchMode dispatch = DispatchMode.INLINE;

    /**
     * Messages buffered per flow before the JCSMP delivery thread blocks, in EXECUTOR dispatch.
     * The bound is what preserves the broker's flow control; raising it trades heap for burst
     * tolerance.
     */
    private int dispatchQueueCapacity = 256;

    /**
     * Hold the JVM open while this container runs. Needed by consumer-only applications that have
     * no web server, because every JCSMP thread is a daemon thread. Turn it off for containers that
     * should not by themselves keep an application alive, such as a request-reply reply container.
     */
    private boolean keepAlive = true;

    private Duration shutdownTimeout = Duration.ofSeconds(10);

    private int phase = Integer.MAX_VALUE - 100;

    private final Endpoint endpoint = new Endpoint();

    /** Properties applied when the container provisions its endpoint. */
    @Data
    public static class Endpoint {

        private AccessType accessType = AccessType.NONEXCLUSIVE;

        private Permission permission = Permission.MODIFY_TOPIC;

        /** Endpoint quota in MB. */
        private int quotaMb = 100;

        private boolean respectsTtl = true;

        public EndpointProperties toEndpointProperties() {
            EndpointProperties properties = new EndpointProperties();
            properties.setAccessType(this.accessType.value());
            properties.setPermission(this.permission.value());
            properties.setQuota(this.quotaMb);
            properties.setRespectsMsgTTL(this.respectsTtl);
            return properties;
        }
    }

    /** Where the listener is invoked. */
    public enum DispatchMode {

        /**
         * Invoke the listener on the JCSMP delivery thread. Lowest latency, and the only correct
         * choice for transacted flows, whose session must be driven by the delivering thread.
         */
        INLINE,

        /**
         * Invoke the listener on a Spring {@code AsyncTaskExecutor}, one invoker task per flow, as
         * {@code DefaultMessageListenerContainer} does for JMS. The delivery thread stays free to
         * receive while the listener works, and listeners run on Spring managed threads. Not
         * available for transactional containers.
         */
        EXECUTOR
    }

    /** Endpoint access type; {@code NONEXCLUSIVE} is required for concurrency greater than one. */
    public enum AccessType {

        EXCLUSIVE(EndpointProperties.ACCESSTYPE_EXCLUSIVE),
        NONEXCLUSIVE(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

        private final int value;

        AccessType(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }

    /** Endpoint permission granted to other clients. */
    public enum Permission {

        NONE(EndpointProperties.PERMISSION_NONE),
        READ_ONLY(EndpointProperties.PERMISSION_READ_ONLY),
        CONSUME(EndpointProperties.PERMISSION_CONSUME),
        MODIFY_TOPIC(EndpointProperties.PERMISSION_MODIFY_TOPIC),
        DELETE(EndpointProperties.PERMISSION_DELETE);

        private final int value;

        Permission(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }
}
