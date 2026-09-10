package org.cris.prs.messaging.solace.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as the target of a Solace listener container &mdash; the direct counterpart of
 * {@code @KafkaListener}.
 *
 * <p>The method may take the converted payload, a {@code Message<?>}, a {@code SolaceRecord<?>},
 * the raw {@code BytesXMLMessage}, and {@code @Header}/{@code @Headers} annotated parameters. A
 * non-void return value is published back to the destination in the request's {@code replyTo}
 * field (or to {@link #replyDestination()} when set), which is what makes request-reply work
 * without any explicit output binding.</p>
 *
 * <p>All attributes are strings so that they can be supplied through property placeholders, e.g.
 * {@code concurrency = "${app.concurrency:10}"}.</p>
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SolaceListener {

    /**
     * Container id; generated when not set.
     *
     * @return the container id
     */
    String id() default "";

    /**
     * {@code PUBLISH_SUBSCRIBE}, {@code POINT_TO_POINT} or {@code REQUEST_REPLY}. Sets the endpoint
     * wiring that realises the pattern &mdash; a per-instance endpoint for fan-out, one shared
     * non-exclusive endpoint for competing consumers. Attributes set explicitly here always win;
     * empty leaves every default to the container factory.
     *
     * @return the exchange pattern name, or empty for none
     */
    String pattern() default "";

    /**
     * Topic subscriptions, e.g. {@code "orders/created"} or {@code "orders/>"}.
     *
     * @return the topic subscriptions
     */
    String[] topics() default {};

    /**
     * Queue (endpoint) name. Ignored when {@link #endpointMode()} is {@code DIRECT}.
     *
     * @return the endpoint name
     */
    String queue() default "";

    /**
     * Consumer group, appended to the queue name as {@code <queue>.<group>}.
     *
     * @return the consumer group segment
     */
    String group() default "";

    /**
     * {@code DURABLE_QUEUE}, {@code NON_DURABLE_QUEUE} or {@code DIRECT}; empty uses the default.
     *
     * @return the endpoint mode name, or empty for the container default
     */
    String endpointMode() default "";

    /**
     * Number of consumer flows bound to the endpoint.
     *
     * @return the number of consumer flows, or empty for the container default
     */
    String concurrency() default "";

    /**
     * Broker side selector over the message's user properties.
     *
     * @return the broker-side selector, or empty for none
     */
    String selector() default "";

    /**
     * Consume and reply inside a Solace local transaction.
     *
     * @return whether to consume transactionally, or empty for the container default
     */
    String transactional() default "";

    /**
     * {@code INLINE} to invoke this listener on the JCSMP delivery thread, {@code EXECUTOR} to
     * invoke it on the Solace listener task executor. Empty uses the default. {@code EXECUTOR}
     * cannot be combined with {@code transactional = "true"}.
     *
     * @return the dispatch mode name, or empty for the container default
     */
    String dispatch() default "";

    /**
     * What to do with a message whose listener throws: {@code ACCEPTED}, {@code FAILED},
     * {@code REJECTED} or {@code NONE}. Empty inherits {@code solace.listener.error-outcome}.
     *
     * <p>{@code FAILED} hands the message back for redelivery and counts the attempt;
     * {@code REJECTED} sends it straight to the dead message queue without consuming redelivery
     * attempts. Ignored when {@link #transactional()} is set, where the rollback governs
     * redelivery.</p>
     *
     * @return the settlement outcome name, or empty to inherit
     */
    String errorOutcome() default "";

    /**
     * Where to replay from on every bind: {@code BEGINNING}, or an ISO-8601 instant such as
     * {@code 2026-08-23T10:15:30Z}. Empty means live delivery only.
     *
     * <p>Replay affects the <b>whole endpoint</b>, so on a shared queue this re-delivers to every
     * consumer of it. Prefer the runtime operation
     * {@code DefaultSolaceMessageListenerContainer.replay(...)} for anything operational &mdash;
     * leaving a start point in an annotation means every restart replays again.</p>
     *
     * @return the replay start point, or empty for live delivery only
     */
    String replayFrom() default "";

    /**
     * Share one endpoint with the other listeners that declare the same {@link #queue()} and
     * {@link #group()}, routing each message to the method whose {@link #topics()} matched.
     *
     * <p>Without this, twenty listeners on related topics cost twenty queues, twenty binds and twenty
     * sets of provisioning. With it they cost one, and each method still receives its own payload
     * type.</p>
     *
     * <p>Opt-in rather than implicit, because merging listeners that merely happen to share a queue
     * name would change what an existing application does. Every listener in a group must declare it,
     * and the group's container settings come from the first &mdash; a conflicting {@code pattern},
     * {@code endpointMode}, {@code concurrency}, {@code transactional} or {@code selector} on a later
     * one fails at startup rather than being silently ignored.</p>
     *
     * <p>The first matching subscription wins, in declaration order, so overlapping subscriptions
     * resolve by order and not by specificity.</p>
     *
     * @return {@code "true"} to share an endpoint with the rest of its group
     */
    String topicDispatch() default "";

    /**
     * Whether the container starts with the application context.
     *
     * @return whether to start automatically, or empty for the container default
     */
    String autoStartup() default "";

    /**
     * Give this instance its own endpoint by appending the instance id to the queue name.
     *
     * @return whether the endpoint name carries the instance id
     */
    String appendInstanceIdToQueue() default "";

    /**
     * Give this instance its own subscription by appending the instance id as a topic level.
     *
     * @return whether each subscription carries the instance id
     */
    String appendInstanceIdToTopics() default "";

    /**
     * Fixed reply destination; when empty, replies follow the request's {@code replyTo}.
     *
     * @return the fixed reply destination, or empty to follow the request
     */
    String replyDestination() default "";

    /**
     * Bean name of the {@code SolaceListenerContainerFactory} to use.
     *
     * @return the container factory bean name, or empty for the default
     */
    String containerFactory() default "";
}
