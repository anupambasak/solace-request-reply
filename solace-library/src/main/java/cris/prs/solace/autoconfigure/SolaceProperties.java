package cris.prs.solace.autoconfigure;

import com.solacesystems.jcsmp.DeliveryMode;
import cris.prs.messaging.solace.listener.ContainerProperties;
import cris.prs.messaging.solace.requestreply.ReplyEndpointSpec;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;


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
    @EqualsAndHashCode(callSuper = true)
    public static class RequestReply extends ReplyEndpointSpec {

        /** Create request-reply defaults with every value at its documented default. */
        public RequestReply() {
        }

        /**
         * Create the request-reply template and its reply container. Set {@code false} on services
         * that only consume and never originate requests, so no reply endpoint is bound.
         */
        private boolean enabled = true;
    }
}
