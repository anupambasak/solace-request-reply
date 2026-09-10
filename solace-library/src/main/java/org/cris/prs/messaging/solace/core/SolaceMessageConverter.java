package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;

/**
 * Converts payloads to and from Solace messages &mdash; the analogue of a Kafka
 * {@code Serializer}/{@code Deserializer} pair.
 *
 * <p>A converter is responsible for the message <em>body</em> only. Headers and native message
 * fields are applied separately by a {@link SolaceHeaderMapper}, so the two concerns can be replaced
 * independently: a Protobuf body with the standard header mapping, or JSON with a custom one.</p>
 *
 * <p>Implementations must be thread safe: one converter serves every template and listener
 * container in the application.</p>
 *
 * @see JacksonSolaceMessageConverter
 */
public interface SolaceMessageConverter {

    /**
     * Create a Solace message carrying the given payload.
     *
     * <p>Delivery mode, expiry, priority and headers are applied by the caller afterwards, so an
     * implementation only has to choose a message type and write the body.</p>
     *
     * @param payload the payload to serialise; may be {@code null}, which should produce an empty body
     * @return a new message carrying the serialised payload
     * @throws SolaceMessagingException if the payload cannot be serialised
     */
    XMLMessage toMessage(Object payload);

    /**
     * Convert the body of a received message into the requested type.
     *
     * @param message    the received message, never {@code null}
     * @param targetType the type wanted by the listener. {@code null}, {@code Object} or a
     *                   {@code BytesXMLMessage} type should yield the raw message, so that a
     *                   listener can opt out of conversion entirely
     * @return the converted payload
     * @throws SolaceMessagingException if the body cannot be converted into {@code targetType}
     */
    Object fromMessage(BytesXMLMessage message, Class<?> targetType);
}
