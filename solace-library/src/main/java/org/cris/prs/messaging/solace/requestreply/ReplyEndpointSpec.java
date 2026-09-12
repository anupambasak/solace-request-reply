package org.cris.prs.messaging.solace.requestreply;

import com.solacesystems.jcsmp.DeliveryMode;
import org.cris.prs.messaging.solace.core.EndpointMode;
import lombok.Data;

import java.time.Duration;

/**
 * Describes one reply destination and the template that consumes it.
 *
 * <p>An application normally has a single reply destination, shared by every service it calls: the
 * reply channel belongs to the <em>requester</em>, and the correlation id is what returns each reply
 * to its request. One endpoint per instance, rather than one per instance per service, is both the
 * conventional arrangement and much the cheaper one.</p>
 *
 * <p>A second spec is worth the extra endpoint when one of these applies:</p>
 * <ul>
 *   <li><b>Head-of-line blocking</b> &mdash; a high-volume service's replies delay a latency
 *       sensitive one's on the shared flow. Raising {@code concurrency} on the shared endpoint is
 *       the cheaper fix; a separate endpoint is the thorough one.</li>
 *   <li><b>Different trust domains</b> &mdash; anything able to publish to a shared reply topic
 *       could forge a reply for another service, given a guessed correlation id.</li>
 *   <li><b>Blast radius</b> &mdash; a stalled or full reply endpoint stops every conversation that
 *       shares it, not just one service's.</li>
 *   <li><b>Observability</b> &mdash; queue depth and latency per service, rather than in aggregate.</li>
 * </ul>
 *
 * @see ReplyingSolaceTemplateFactory
 */
@Data
public class ReplyEndpointSpec {

    /** Create a spec with every value at its documented default. */
    public ReplyEndpointSpec() {
    }

    /**
     * Identifies the reply container in logs and in the listener registry. Must be unique across the
     * application when more than one reply destination is in use.
     */
    private String id = "solaceReplyContainer";

    /**
     * Base reply topic. Replies arrive on {@code <prefix>/<instance-id>} when
     * {@code appendInstanceId} is set.
     */
    private String replyTopicPrefix = "reply";

    /**
     * Append the instance id to the reply topic and endpoint name. Turning this off makes every
     * instance share one reply destination, so replies reach the wrong requester.
     */
    private boolean appendInstanceId = true;

    /** How the reply endpoint binds. */
    private EndpointMode endpointMode = EndpointMode.NON_DURABLE_QUEUE;

    /** Base name of the reply endpoint; {@code null} derives it from the topic prefix. */
    private String replyQueue;

    /** Optional group segment, for a shared durable reply endpoint. */
    private String replyGroup;

    /** Optional broker-side selector on the reply endpoint. */
    private String selector;

    /**
     * Flows consuming replies. Above one the endpoint is provisioned non-exclusive, which is what
     * lets several replies be converted and completed in parallel.
     *
     * <p>Only meaningful with {@code endpointMode = DURABLE_QUEUE}. The default
     * {@code NON_DURABLE_QUEUE} is a temporary endpoint that accepts exactly one flow, so a higher
     * value is clamped to 1 with a warning. Set {@code replyQueue} (and usually {@code replyGroup})
     * together with a durable mode before raising this.</p>
     */
    private int concurrency = 1;

    /** How long a request waits before its future fails with {@link SolaceReplyTimeoutException}. */
    private Duration replyTimeout = Duration.ofSeconds(30);

    /** Delivery mode used for requests published through the resulting template. */
    private DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;

    /**
     * Expiry in milliseconds for requests published through the resulting template; {@code 0} means no
     * expiry. Unset &mdash; the default &mdash; inherits {@code solace.template.time-to-live}.
     *
     * <p>Worth setting on a request-reply template: a request nobody is waiting for any more is still a
     * request the responder will answer. With an expiry at or just under {@code replyTimeout}, and
     * {@code respects-ttl} on the request endpoint, the broker stops delivering a request once its
     * requester has given up, and moves it to the dead message queue if it is
     * {@linkplain #dmqEligible DMQ eligible} &mdash; instead of a listener processing it minutes later
     * and publishing a reply that arrives to no outstanding request.</p>
     */
    private Long timeToLive;

    /**
     * Priority of requests published through the resulting template. Unset inherits
     * {@code solace.template.priority}.
     */
    private Integer priority;

    /**
     * Move expired or undeliverable requests to the dead message queue. Unset inherits
     * {@code solace.template.dmq-eligible}.
     */
    private Boolean dmqEligible;
}
