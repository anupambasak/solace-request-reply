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

    /** Container id; generated when not set. */
    String id() default "";

    /**
     * {@code PUBLISH_SUBSCRIBE}, {@code POINT_TO_POINT} or {@code REQUEST_REPLY}. Sets the endpoint
     * wiring that realises the pattern &mdash; a per-instance endpoint for fan-out, one shared
     * non-exclusive endpoint for competing consumers. Attributes set explicitly here always win;
     * empty leaves every default to the container factory.
     */
    String pattern() default "";

    /** Topic subscriptions, e.g. {@code "bkg/trn"} or {@code "bkg/trn/>"}. */
    String[] topics() default {};

    /** Queue (endpoint) name. Ignored when {@link #endpointMode()} is {@code DIRECT}. */
    String queue() default "";

    /** Consumer group, appended to the queue name as {@code <queue>.<group>}. */
    String group() default "";

    /** {@code DURABLE_QUEUE}, {@code NON_DURABLE_QUEUE} or {@code DIRECT}; empty uses the default. */
    String endpointMode() default "";

    /** Number of consumer flows bound to the endpoint. */
    String concurrency() default "";

    /** Broker side selector over the message's user properties. */
    String selector() default "";

    /** Consume and reply inside a Solace local transaction. */
    String transactional() default "";

    /**
     * {@code INLINE} to invoke this listener on the JCSMP delivery thread, {@code EXECUTOR} to
     * invoke it on the Solace listener task executor. Empty uses the default. {@code EXECUTOR}
     * cannot be combined with {@code transactional = "true"}.
     */
    String dispatch() default "";

    String autoStartup() default "";

    /** Give this instance its own endpoint by appending the instance id to the queue name. */
    String appendInstanceIdToQueue() default "";

    /** Give this instance its own subscription by appending the instance id as a topic level. */
    String appendInstanceIdToTopics() default "";

    /** Fixed reply destination; when empty, replies follow the request's {@code replyTo}. */
    String replyDestination() default "";

    /** Bean name of the {@code SolaceListenerContainerFactory} to use. */
    String containerFactory() default "";
}
