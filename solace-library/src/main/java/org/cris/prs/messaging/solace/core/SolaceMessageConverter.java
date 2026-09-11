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
 * <p>A converter may also write SDT user properties &mdash; a schema id, say. Those belong to the
 * converter: {@link DefaultSolaceHeaderMapper} never overwrites a user property the converter has
 * already set, so a stale header copied from an inbound message cannot corrupt it.</p>
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
     * Create a Solace message carrying the given payload, for publication to a known destination.
     *
     * <p>This is the method {@link SolaceTemplate} calls. A converter whose wire format depends on
     * where the message is going &mdash; a schema registry resolving the schema from the topic, for
     * instance &mdash; overrides it; every other converter inherits this default, which ignores the
     * destination, so an existing implementation keeps compiling and behaving exactly as before.</p>
     *
     * @param payload     the payload to serialise; may be {@code null}, which should produce an empty body
     * @param destination the destination exactly as it was given to {@code send}, so a queue carries
     *                    its {@value DefaultSolaceHeaderMapper#QUEUE_PREFIX} prefix &mdash; resolve it
     *                    with {@link DefaultSolaceHeaderMapper#toDestination(Object)}. {@code null}
     *                    when the message is built without a destination
     * @return a new message carrying the serialised payload
     * @throws SolaceMessagingException if the payload cannot be serialised
     */
    default XMLMessage toMessage(Object payload, String destination) {
        return toMessage(payload);
    }

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
