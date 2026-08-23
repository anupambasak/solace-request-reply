package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default header mapper. Headers named after the constants in {@link SolaceHeaders} are written to
 * the corresponding native Solace message field; everything else becomes an SDT user property, so
 * it remains usable in broker side selectors.
 */
@Slf4j
public class DefaultSolaceHeaderMapper implements SolaceHeaderMapper {

    /** Create a header mapper. */
    public DefaultSolaceHeaderMapper() {
    }


    /** Prefix that turns a destination name into a queue rather than a topic. */
    public static final String QUEUE_PREFIX = "queue:";

    /** Headers that are never written to an outbound message. */
    private static final Set<String> IGNORED_HEADERS = Set.of(
            SolaceHeaders.RAW_MESSAGE,
            SolaceHeaders.DESTINATION,
            SolaceHeaders.REDELIVERED,
            SolaceHeaders.DELIVERY_COUNT,
            SolaceHeaders.TARGET_DESTINATION,
            "id",
            "timestamp");

    /** {@inheritDoc} */
    @Override
    public void fromHeaders(Map<String, Object> headers, XMLMessage message) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        SDTMap userProperties = null;
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value == null || isIgnored(name)) {
                continue;
            }
            switch (name) {
                case SolaceHeaders.CORRELATION_ID -> message.setCorrelationId(value.toString());
                case SolaceHeaders.APPLICATION_MESSAGE_ID -> message.setApplicationMessageId(value.toString());
                case SolaceHeaders.SENDER_TIMESTAMP -> message.setSenderTimestamp(asLong(value));
                case SolaceHeaders.TIME_TO_LIVE -> message.setTimeToLive(asLong(value));
                case SolaceHeaders.PRIORITY -> message.setPriority(asInt(value));
                case SolaceHeaders.REPLY_TO -> message.setReplyTo(toDestination(value));
                default -> {
                    if (userProperties == null) {
                        userProperties = message.getProperties() != null
                                ? message.getProperties()
                                : JCSMPFactory.onlyInstance().createMap();
                    }
                    putUserProperty(userProperties, name, value);
                }
            }
        }
        if (userProperties != null) {
            message.setProperties(userProperties);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Includes the native correlation id, reply-to, destination, application message id, sender
     * timestamp and redelivered flag, followed by every SDT user property.</p>
     */
    @Override
    public Map<String, Object> toHeaders(BytesXMLMessage message) {
        Map<String, Object> headers = new LinkedHashMap<>();
        if (message.getCorrelationId() != null) {
            headers.put(SolaceHeaders.CORRELATION_ID, message.getCorrelationId());
        }
        if (message.getReplyTo() != null) {
            headers.put(SolaceHeaders.REPLY_TO, message.getReplyTo().getName());
        }
        if (message.getDestination() != null) {
            headers.put(SolaceHeaders.DESTINATION, message.getDestination().getName());
        }
        if (message.getApplicationMessageId() != null) {
            headers.put(SolaceHeaders.APPLICATION_MESSAGE_ID, message.getApplicationMessageId());
        }
        headers.put(SolaceHeaders.SENDER_TIMESTAMP, message.getSenderTimestamp());
        headers.put(SolaceHeaders.REDELIVERED, message.getRedelivered());
        headers.put(SolaceHeaders.DELIVERY_COUNT, deliveryCountOf(message));

        SDTMap properties = message.getProperties();
        if (properties != null) {
            for (String key : properties.keySet()) {
                try {
                    headers.put(key, properties.get(key));
                }
                catch (Exception ex) {
                    log.debug("Unable to read SDT user property '{}'", key, ex);
                }
            }
        }
        return headers;
    }

    /**
     * Read a message's delivery count, tolerating brokers and client versions that do not report one.
     *
     * <p>The count is a broker feature negotiated per message: {@code getDeliveryCount()} throws
     * {@code UnsupportedOperationException} where it is unavailable, so it must always be guarded by
     * {@code isDeliveryCountSupported()}. Both are caught here, and the whole call is wrapped, so an
     * older broker degrades to {@code -1} rather than failing every message.</p>
     *
     * @param message the received message, or {@code null}
     * @return the delivery count &mdash; {@code 1} on a first delivery &mdash; or {@code -1} when it
     *         is not supported
     */
    public static int deliveryCountOf(BytesXMLMessage message) {
        if (message == null) {
            return -1;
        }
        try {
            return message.isDeliveryCountSupported() ? message.getDeliveryCount() : -1;
        }
        catch (Exception ex) {
            log.debug("Unable to read the delivery count", ex);
            return -1;
        }
    }

    /**
     * Convert a destination name into a Solace {@link Destination}.
     *
     * @param value a {@code Destination}, or a name. A name prefixed {@value #QUEUE_PREFIX} resolves
     *              to a queue; anything else to a topic
     * @return the resolved destination
     */
    public static Destination toDestination(Object value) {
        if (value instanceof Destination destination) {
            return destination;
        }
        String name = value.toString();
        if (name.startsWith(QUEUE_PREFIX)) {
            return JCSMPFactory.onlyInstance().createQueue(name.substring(QUEUE_PREFIX.length()));
        }
        return JCSMPFactory.onlyInstance().createTopic(name);
    }

    private boolean isIgnored(String name) {
        return IGNORED_HEADERS.contains(name);
    }

    private void putUserProperty(SDTMap map, String name, Object value) {
        try {
            switch (value) {
                case String string -> map.putString(name, string);
                case Integer integer -> map.putInteger(name, integer);
                case Long longValue -> map.putLong(name, longValue);
                case Double doubleValue -> map.putDouble(name, doubleValue);
                case Boolean booleanValue -> map.putBoolean(name, booleanValue);
                case byte[] bytes -> map.putBytes(name, bytes);
                default -> map.putString(name, value.toString());
            }
        }
        catch (Exception ex) {
            log.debug("Unable to write SDT user property '{}'", name, ex);
        }
    }

    private static long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : Long.parseLong(value.toString());
    }

    private static int asInt(Object value) {
        return value instanceof Number number ? number.intValue() : Integer.parseInt(value.toString());
    }

    /**
     * Copy a header map without the framework-internal entries.
     *
     * @param headers the headers to copy
     * @return a mutable copy with {@code solace_rawMessage}, {@code id} and {@code timestamp} removed
     */
    public static Map<String, Object> sanitize(Map<String, Object> headers) {
        Map<String, Object> copy = new HashMap<>(headers);
        copy.remove(SolaceHeaders.RAW_MESSAGE);
        copy.remove("id");
        copy.remove("timestamp");
        return copy;
    }
}
