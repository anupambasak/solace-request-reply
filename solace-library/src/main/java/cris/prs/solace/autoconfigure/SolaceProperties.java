package cris.prs.solace.autoconfigure;

import com.solacesystems.jcsmp.DeliveryMode;
import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.listener.ContainerProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration for the Solace messaging abstraction, bound from {@code solace.*}.
 *
 * <p>The broker connection itself is configured by the {@code solace-java-spring-boot-starter}
 * under {@code solace.java.*} (host, msgVpn, clientUsername, clientPassword, ...).</p>
 */
@Data
@ConfigurationProperties(prefix = "solace")
public class SolaceProperties {

    /**
     * Identifier for this application instance, used to make reply destinations unique per pod.
     * Defaults to {@code $HOSTNAME}, then {@code $POD_NAME}, then the local host name.
     */
    private String instanceId;

    private final Template template = new Template();

    private final Listener listener = new Listener();

    private final RequestReply requestReply = new RequestReply();

    /** Create the properties with every value at its documented default. */
    public SolaceProperties() {
    }

    /** Defaults applied to the auto-configured {@code SolaceTemplate}. */
    @Data
    public static class Template {

        /** Create template defaults with every value at its documented default. */
        public Template() {
        }

        /** Destination used by {@code send(payload)} when none is given. */
        private String defaultDestination;

        /** PERSISTENT for guaranteed messaging, DIRECT for at-most-once. */
        private DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;

        /** Message expiry in milliseconds; 0 means no expiry. */
        private long timeToLive = 0L;

        private Integer priority;

        /** Move expired or undeliverable messages to the dead message queue. */
        private boolean dmqEligible = true;
    }

    /** Defaults applied to every {@code @SolaceListener} container. */
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class Listener extends ContainerProperties {

        /** Create listener defaults with every value at its documented default. */
        public Listener() {
        }
    }

    /** Configuration of the auto-configured {@code ReplyingSolaceTemplate}. */
    @Data
    public static class RequestReply {

        /** Create request-reply defaults with every value at its documented default. */
        public RequestReply() {
        }

        /** Create the ReplyingSolaceTemplate and its reply container. */
        private boolean enabled = true;

        /**
         * Base reply topic. The instance id is appended as a further topic level when
         * {@code append-instance-id} is set, e.g. {@code app/reply/client-7d9f-abcde}.
         */
        private String replyTopicPrefix = "reply";

        /** Give every pod its own reply topic level and its own reply endpoint. */
        private boolean appendInstanceId = true;

        /**
         * How the reply endpoint is bound: NON_DURABLE_QUEUE (temporary, guaranteed, cleaned up by
         * the broker on disconnect), DURABLE_QUEUE (survives restarts) or DIRECT (non-persistent).
         */
        private EndpointMode endpointMode = EndpointMode.NON_DURABLE_QUEUE;

        /** Base name of the reply endpoint; defaults to the reply topic prefix with dots. */
        private String replyQueue;

        /** Optional consumer group segment for a shared durable reply endpoint. */
        private String replyGroup;

        /** Optional broker side selector on the reply endpoint. */
        private String selector;

        /** Number of flows consuming replies. */
        private int concurrency = 1;

        /** How long a request waits for its reply before the future fails. */
        private Duration replyTimeout = Duration.ofSeconds(30);

        /** Delivery mode used for requests; replies follow the template defaults. */
        private DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;
    }
}
