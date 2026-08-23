package cris.prs.messaging.solace.annotation;

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
