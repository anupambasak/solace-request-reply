package org.cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import org.cris.prs.messaging.solace.core.EndpointMode;
import org.cris.prs.messaging.solace.core.SettlementOutcome;
import lombok.Data;

import java.time.Duration;

/** Tuning knobs for a listener container, analogous to Kafka's {@code ContainerProperties}. */
@Data
public class ContainerProperties {

    /** Create container properties with every value at its documented default. */
    public ContainerProperties() {
    }


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
     *
     * @deprecated superseded by {@code errorOutcome}, which expresses the same two answers and two
     *         more. Still honoured, but only when {@code errorOutcome} is unset: {@code true} maps to
     *         {@code ACCEPTED} and {@code false} to {@code NONE}.
     */
    @Deprecated(since = "0.2.0", forRemoval = true)
    private boolean ackOnError = true;

    /**
     * What to do with a message whose listener threw.
     *
     * <p>Unset by default, in which case the deprecated {@code ackOnError} decides:
     * {@code true} behaves as {@code ACCEPTED}, {@code false} as {@code NONE}. Setting this
     * explicitly takes precedence and is the preferred way to configure error handling.</p>
     *
     * <p>{@code FAILED} hands the message back for redelivery and increments its delivery count;
     * {@code REJECTED} sends it to the dead message queue immediately, without consuming redelivery
     * attempts. Both require the flow to negotiate the outcome at bind time, which
     * {@code negativeAcknowledgement} handles. Ignored on a transactional container, where the
     * rollback governs redelivery.</p>
     *
     * @see org.cris.prs.messaging.solace.core.SettlementOutcome
     */
    private SettlementOutcome errorOutcome;

    /**
     * Negotiate the negative settlement outcomes on every flow this container binds.
     *
     * <p>A flow must declare at bind time which outcomes it may send, so {@code FAILED} and
     * {@code REJECTED} are unavailable unless they were requested up front. Unset by default, in
     * which case the container derives it: negotiation happens when the resolved
     * {@code errorOutcome} is {@code FAILED} or {@code REJECTED}, and not otherwise.</p>
     *
     * <p>Set it to {@code true} explicitly when a {@code SolaceListenerErrorHandler} decides the
     * outcome per message &mdash; the container cannot know in advance what such a handler will
     * return. Set it to {@code false} to force the old behaviour against a broker or client library
     * that does not support settlement outcomes, where requesting them fails the bind.</p>
     */
    private Boolean negativeAcknowledgement;

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

    /**
     * Transacted sessions Solace allows on one client connection. A transactional container takes
     * one per flow, so it opens its own connection and refuses to start if {@code concurrency}
     * exceeds this. Matches the broker's client-profile default; raise both together if you raise
     * it on the broker.
     */
    private int maxTransactedSessionsPerConnection = 10;

    private Duration shutdownTimeout = Duration.ofSeconds(10);

    private int phase = Integer.MAX_VALUE - 100;

    private final Endpoint endpoint = new Endpoint();

    private final Flow flow = new Flow();

    /**
     * Tuning applied to every consumer flow the container binds.
     *
     * <p>Every value is nullable and <strong>unset by default</strong>: a property is applied to
     * {@code ConsumerFlowProperties} only when it has been given a value, so leaving this block empty
     * means JCSMP's own defaults, exactly as before this block existed.</p>
     *
     * <p>These are per-flow settings, not per-endpoint ones, so unlike
     * {@link ContainerProperties.Endpoint} they take effect on every bind rather than only when an
     * endpoint is first provisioned.</p>
     */
    @Data
    public static class Flow {

        /** Create the flow tuning with everything unset, meaning JCSMP defaults. */
        public Flow() {
        }

        /**
         * Messages the broker may have in flight to this flow before waiting for acknowledgement.
         *
         * <p>The primary throughput knob for guaranteed messaging. JCSMP defaults to 255; raising it
         * helps a fast consumer on a high-latency link, and costs memory on the broker per flow.
         * Lowering it tightens the bound on how many messages can be lost to a redelivery after a
         * failure.</p>
         */
        private Integer transportWindowSize;

        /**
         * Fraction of the transport window, as a percentage, at which the flow acknowledges.
         *
         * <p>Trades acknowledgement round-trips against redelivery risk: a higher threshold means
         * fewer acknowledgements and more messages redelivered if the flow drops. JCSMP defaults to
         * 60.</p>
         */
        private Integer ackThreshold;

        /**
         * How long the flow waits before acknowledging, when the threshold has not been reached.
         *
         * <p>The floor on acknowledgement latency for a slow trickle of messages. JCSMP defaults to
         * one second.</p>
         */
        private Duration ackTimer;

        /** Maximum messages acknowledged in one transmission. JCSMP chooses a default. */
        private Integer windowedAckMaxSize;

        /**
         * How many times JCSMP retries a lost flow before giving up and reporting {@code FLOW_DOWN}.
         *
         * <p>{@code -1} retries forever. This is flow-level reconnection, separate from the session
         * reconnection configured under {@code solace.java.*}.</p>
         */
        private Integer reconnectTries;

        /** How long JCSMP waits between flow reconnection attempts. */
        private Duration reconnectRetryInterval;

        /**
         * Ask the broker to say when this flow becomes the active consumer on an exclusive endpoint.
         *
         * <p>Unset derives it: enabled when the endpoint's access type is {@code EXCLUSIVE}, where
         * the notification is meaningful, and not otherwise. Without it a standby instance has no way
         * to learn that it has taken over &mdash; which makes this the whole basis of leader election
         * over an exclusive endpoint.</p>
         *
         * <p>Set {@code false} to suppress the extra {@code FLOW_ACTIVE}/{@code FLOW_INACTIVE} events
         * on an exclusive endpoint you do not use for leadership.</p>
         */
        private Boolean activeFlowIndication;

        /**
         * Suppress delivery to this flow of messages published by the <em>same client connection</em>.
         *
         * <p>Solace matches on the connection, not the application: a publisher and a consumer in one
         * process share the library's session by default, so a service that both publishes to a topic
         * and subscribes to it would otherwise receive its own messages. That is right for a work
         * queue several instances share, and wrong for a broadcast an instance sends to its
         * peers.</p>
         *
         * <p>Two things to know before turning it on. A <b>transactional</b> container gets its own
         * connection, so its publishes and consumes are already on different connections and this has
         * no effect there. And it is a <b>per-flow</b> filter, so on a shared durable queue the
         * message is not delivered to this instance but is still delivered to another &mdash; it
         * suppresses local delivery, it does not discard the message.</p>
         */
        private Boolean noLocal;

        /**
         * Apply everything that has been set to a flow's properties.
         *
         * <p>Only non-null values are applied, so this is a no-op on an untouched block.</p>
         *
         * @param flowProperties the properties being built for a flow
         * @param exclusiveEndpoint whether the endpoint is exclusive, used to derive
         *                          {@code activeFlowIndication} when it is unset
         */
        public void applyTo(ConsumerFlowProperties flowProperties, boolean exclusiveEndpoint) {
            if (this.transportWindowSize != null) {
                flowProperties.setTransportWindowSize(this.transportWindowSize);
            }
            if (this.ackThreshold != null) {
                flowProperties.setAckThreshold(this.ackThreshold);
            }
            if (this.ackTimer != null) {
                flowProperties.setAckTimerInMsecs((int) this.ackTimer.toMillis());
            }
            if (this.windowedAckMaxSize != null) {
                flowProperties.setWindowedAckMaxSize(this.windowedAckMaxSize);
            }
            if (this.reconnectTries != null) {
                flowProperties.setReconnectTries(this.reconnectTries);
            }
            if (this.reconnectRetryInterval != null) {
                flowProperties.setReconnectRetryIntervalInMsecs(
                        (int) this.reconnectRetryInterval.toMillis());
            }
            if (this.noLocal != null) {
                flowProperties.setNoLocal(this.noLocal);
            }
            flowProperties.setActiveFlowIndication(this.activeFlowIndication != null
                    ? this.activeFlowIndication
                    : exclusiveEndpoint);
        }
    }

    /** Properties applied when the container provisions its endpoint. */
    @Data
    public static class Endpoint {

        /** Create endpoint properties with every value at its documented default. */
        public Endpoint() {
        }


        private AccessType accessType = AccessType.NONEXCLUSIVE;

        private Permission permission = Permission.MODIFY_TOPIC;

        /** Endpoint quota in MB. */
        private int quotaMb = 100;

        private boolean respectsTtl = true;

        /**
         * Redeliveries allowed before the broker gives up on a message and moves it to the dead
         * message queue. {@code 0} keeps the broker default of retrying forever, which turns a
         * message the listener can never handle into an endless rollback loop. Valid range 0-255.
         */
        private int maxRedeliveryCount = 0;

        private final DeadMessageQueue deadMessageQueue = new DeadMessageQueue();

        /**
         * Build the JCSMP endpoint properties, using the configured access type.
         *
         * @return properties applied when the endpoint is provisioned and when a flow binds
         */
        public EndpointProperties toEndpointProperties() {
            return toEndpointProperties(null);
        }

        /**
         * Build the JCSMP endpoint properties, letting an exchange pattern impose the access type.
         *
         * <p>{@code maxRedeliveryCount} is applied only when greater than zero, so leaving it at the
         * default does not override the broker's own.</p>
         *
         * @param accessTypeOverride the access type the endpoint's exchange pattern requires, or
         *                           {@code null} to use the configured default
         * @return properties applied when the endpoint is provisioned and when a flow binds
         */
        public EndpointProperties toEndpointProperties(AccessType accessTypeOverride) {
            EndpointProperties properties = new EndpointProperties();
            properties.setAccessType(
                    (accessTypeOverride != null ? accessTypeOverride : this.accessType).value());
            properties.setPermission(this.permission.value());
            properties.setQuota(this.quotaMb);
            properties.setRespectsMsgTTL(this.respectsTtl);
            if (this.maxRedeliveryCount > 0) {
                properties.setMaxMsgRedelivery(this.maxRedeliveryCount);
            }
            return properties;
        }
    }

    /**
     * The dead message queue that receives messages whose redelivery count is exhausted.
     *
     * <p>Solace has one DMQ per message VPN, and it must carry the well-known name
     * {@code #DEAD_MSG_QUEUE}. A message only reaches it when it was published DMQ eligible
     * ({@code SolaceTemplate} sets that by default), the consuming endpoint has a
     * {@code max-redelivery-count}, and the DMQ exists.</p>
     */
    @Data
    public static class DeadMessageQueue {

        /** Create dead message queue properties with every value at its documented default. */
        public DeadMessageQueue() {
        }


        /** Create the DMQ at container startup if it is missing. */
        private boolean provision = false;

        /** Solace only recognises this name as the dead message queue. */
        private String name = "#DEAD_MSG_QUEUE";

        private int quotaMb = 100;

        private AccessType accessType = AccessType.EXCLUSIVE;

        private Permission permission = Permission.CONSUME;

        /**
         * Build the JCSMP endpoint properties for the dead message queue.
         *
         * @return properties with {@code respectsTTL} disabled, which the broker requires of a DMQ
         */
        public EndpointProperties toEndpointProperties() {
            EndpointProperties properties = new EndpointProperties();
            properties.setAccessType(this.accessType.value());
            properties.setPermission(this.permission.value());
            properties.setQuota(this.quotaMb);
            // The broker rejects a DMQ provisioned with respectTTL enabled
            // (subcode INVALID_PARAMETER_COMBINATION): expiry is what put the message here.
            properties.setRespectsMsgTTL(false);
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

        /** One consumer at a time; further flows are standby at best. Required for fan-out. */
        EXCLUSIVE(EndpointProperties.ACCESSTYPE_EXCLUSIVE),

        /** Several consumers compete for messages. Required for concurrency above one. */
        NONEXCLUSIVE(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

        private final int value;

        AccessType(int value) {
            this.value = value;
        }

        /**
         * The JCSMP constant this value maps to.
         *
         * @return the corresponding {@code EndpointProperties} constant
         */
        public int value() {
            return this.value;
        }
    }

    /** Endpoint permission granted to other clients. */
    public enum Permission {

        /** No access for other clients. */
        NONE(EndpointProperties.PERMISSION_NONE),

        /** Other clients may browse the endpoint but not consume from it. */
        READ_ONLY(EndpointProperties.PERMISSION_READ_ONLY),

        /** Other clients may consume from the endpoint. */
        CONSUME(EndpointProperties.PERMISSION_CONSUME),

        /** Other clients may consume and change the endpoint's topic subscriptions. */
        MODIFY_TOPIC(EndpointProperties.PERMISSION_MODIFY_TOPIC),

        /** Other clients may consume, modify subscriptions, and delete the endpoint. */
        DELETE(EndpointProperties.PERMISSION_DELETE);

        private final int value;

        Permission(int value) {
            this.value = value;
        }

        /**
         * The JCSMP constant this value maps to.
         *
         * @return the corresponding {@code EndpointProperties} constant
         */
        public int value() {
            return this.value;
        }
    }
}
