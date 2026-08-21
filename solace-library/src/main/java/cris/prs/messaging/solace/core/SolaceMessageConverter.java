package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;

/**
 * Converts payloads to and from Solace messages &mdash; the analogue of a Kafka
 * {@code Serializer}/{@code Deserializer} pair.
 */
public interface SolaceMessageConverter {

    /**
     * Create a Solace message carrying the given payload. Headers and native message fields are
     * applied separately by the {@link SolaceHeaderMapper}.
     */
    XMLMessage toMessage(Object payload);

    /** Convert the body of a received message into the requested type. */
    Object fromMessage(BytesXMLMessage message, Class<?> targetType);
}
