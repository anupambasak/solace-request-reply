package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import cris.prs.messaging.solace.core.DefaultSolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceHeaderMapper;
import cris.prs.messaging.solace.core.SolaceHeaders;
import cris.prs.messaging.solace.core.SolaceMessageConverter;
import cris.prs.messaging.solace.core.SolaceRecord;
import cris.prs.messaging.solace.core.SolaceTemplate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for listener adapters; converts the inbound message and publishes whatever the
 * listener returns back to the requester, the way {@code @KafkaListener} does with {@code @SendTo}.
 */
@Slf4j
public abstract class AbstractSolaceListenerAdapter implements SolaceMessageListener {

    /** Converts the message body into the listener's payload type. */
    protected final SolaceMessageConverter messageConverter;

    /** Maps native message fields and SDT user properties into headers. */
    protected final SolaceHeaderMapper headerMapper;

    /** Type the body is converted into before invoking the listener. */
    @Setter
    protected Class<?> payloadType = Object.class;

    /** Template used to publish replies; joins the container's transaction when there is one. */
    @Setter
    protected SolaceTemplate<Object> replyTemplate;

    /** Static reply destination; when unset the request's {@code replyTo} is used. */
    @Setter
    protected String replyDestination;

    /**
     * Create an adapter.
     *
     * @param messageConverter converts the message body into the listener's payload type
     * @param headerMapper     maps native fields and user properties into headers
     */
    protected AbstractSolaceListenerAdapter(SolaceMessageConverter messageConverter,
            SolaceHeaderMapper headerMapper) {
        this.messageConverter = messageConverter;
        this.headerMapper = headerMapper;
    }

    /**
     * Convert the message body into {@code payloadType}.
     *
     * @param message the received message
     * @return the converted payload, or the raw message when the payload type is {@code Object}
     */
    protected Object convertPayload(BytesXMLMessage message) {
        return this.messageConverter.fromMessage(message, this.payloadType);
    }

    /**
     * Build the record handed to listeners that ask for one.
     *
     * @param payload the converted payload
     * @param message the received message
     * @param headers the mapped headers
     * @return a record combining payload, delivery metadata and the raw message
     */
    protected SolaceRecord<Object> toRecord(Object payload, BytesXMLMessage message,
            Map<String, Object> headers) {
        return new SolaceRecord<>(payload,
                message.getDestination() != null ? message.getDestination().getName() : null,
                message.getCorrelationId(),
                message.getReplyTo() != null ? message.getReplyTo().getName() : null,
                headers, message, DefaultSolaceHeaderMapper.deliveryCountOf(message));
    }

    /**
     * Publish the listener's return value as a reply, if there is anywhere to send it.
     *
     * <p>The destination is resolved from, in order: a {@code solace_targetDestination} header on a
     * returned {@code Message}, the configured reply destination, and the request's {@code replyTo}.
     * The correlation id and the {@code instanceId} and {@code requestSendTime} properties are carried
     * across so the requester can match the reply and measure latency.</p>
     *
     * @param result  what the listener returned. {@code null} means a one-way listener and publishes
     *                nothing; a value with nowhere to go is logged and dropped rather than thrown,
     *                since the message itself was handled successfully
     * @param request the message being replied to
     */
    protected void handleResult(Object result, BytesXMLMessage request) {
        if (result == null) {
            return;
        }
        Object payload = result;
        Map<String, Object> replyHeaders = new HashMap<>();
        String destination = this.replyDestination;

        if (result instanceof Message<?> springMessage) {
            payload = springMessage.getPayload();
            replyHeaders.putAll(DefaultSolaceHeaderMapper.sanitize(springMessage.getHeaders()));
            Object target = replyHeaders.remove(SolaceHeaders.TARGET_DESTINATION);
            if (target != null) {
                destination = target.toString();
            }
        }
        if (!StringUtils.hasText(destination) && request.getReplyTo() != null) {
            destination = request.getReplyTo().getName();
        }
        if (!StringUtils.hasText(destination)) {
            log.warn("Listener returned a value but the request carries no replyTo destination "
                    + "and no reply destination is configured; the reply is discarded");
            return;
        }
        replyHeaders.putIfAbsent(SolaceHeaders.CORRELATION_ID, request.getCorrelationId());
        copyThroughUserProperty(request, replyHeaders, SolaceHeaders.INSTANCE_ID);
        copyThroughUserProperty(request, replyHeaders, SolaceHeaders.REQUEST_SEND_TIME);

        if (this.replyTemplate == null) {
            log.warn("Listener returned a value but no reply template is configured; the reply is discarded");
            return;
        }
        this.replyTemplate.send(destination, payload, replyHeaders);
    }

    private void copyThroughUserProperty(BytesXMLMessage request, Map<String, Object> headers, String name) {
        if (headers.containsKey(name) || request.getProperties() == null) {
            return;
        }
        try {
            Object value = request.getProperties().get(name);
            if (value != null) {
                headers.put(name, value);
            }
        }
        catch (Exception ex) {
            log.debug("Unable to copy user property '{}' onto the reply", name, ex);
        }
    }
}
