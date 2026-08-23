package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

/**
 * A received message together with its converted payload &mdash; the Solace analogue of Spring for
 * Apache Kafka's {@code ConsumerRecord}.
 *
 * <p>Declare it as a listener parameter to see the delivery metadata alongside the payload:</p>
 *
 * <pre>{@code
 * @SolaceListener(topics = "events/created", queue = "events")
 * public void onEvent(SolaceRecord<Event> record) {
 *     if (record.isRedelivered()) {
 *         // a previous attempt failed; guard against double processing
 *     }
 * }
 * }</pre>
 *
 * @param <T> the converted payload type
 */
@Getter
@ToString(exclude = "rawMessage")
public class SolaceRecord<T> {

    /** The message body converted to the listener's payload type. */
    private final T payload;

    /** Name of the destination the message was received on; {@code null} if the broker sent none. */
    private final String destination;

    /** The message's native correlation id, or {@code null}. */
    private final String correlationId;

    /**
     * Destination the sender expects a reply on, or {@code null} for a one-way message. A listener
     * that returns a value has its reply published here automatically.
     */
    private final String replyTo;

    /** Native fields and SDT user properties, as mapped by the {@link SolaceHeaderMapper}. */
    private final Map<String, Object> headers;

    /** The underlying Solace message, for anything the mapped view does not expose. */
    private final BytesXMLMessage rawMessage;

    /**
     * How many times the broker has delivered this message.
     *
     * <p>{@code 1} on the first delivery, so a value above 1 means a retry. {@code -1} when the
     * broker or the client library does not support delivery counts &mdash; check for that before
     * branching on the number, or an unsupported broker will look like a first delivery that somehow
     * counts backwards.</p>
     */
    private final int deliveryCount;

    /**
     * Create a record.
     *
     * @param payload       the message body converted to the listener's payload type
     * @param destination   name of the destination the message was received on, or {@code null}
     * @param correlationId the message's native correlation id, or {@code null}
     * @param replyTo       destination the sender expects a reply on, or {@code null} for a
     *                      one-way message
     * @param headers       native fields and SDT user properties
     * @param rawMessage    the underlying Solace message
     */
    public SolaceRecord(T payload, String destination, String correlationId, String replyTo,
            Map<String, Object> headers, BytesXMLMessage rawMessage) {
        this(payload, destination, correlationId, replyTo, headers, rawMessage,
                DefaultSolaceHeaderMapper.deliveryCountOf(rawMessage));
    }

    /**
     * Create a record with an explicit delivery count.
     *
     * @param payload       the message body converted to the listener's payload type
     * @param destination   name of the destination the message was received on, or {@code null}
     * @param correlationId the message's native correlation id, or {@code null}
     * @param replyTo       destination the sender expects a reply on, or {@code null} for a
     *                      one-way message
     * @param headers       native fields and SDT user properties
     * @param rawMessage    the underlying Solace message
     * @param deliveryCount how many times the broker has delivered this message, or {@code -1} when
     *                      delivery counts are not supported
     */
    public SolaceRecord(T payload, String destination, String correlationId, String replyTo,
            Map<String, Object> headers, BytesXMLMessage rawMessage, int deliveryCount) {
        this.payload = payload;
        this.destination = destination;
        this.correlationId = correlationId;
        this.replyTo = replyTo;
        this.headers = headers;
        this.rawMessage = rawMessage;
        this.deliveryCount = deliveryCount;
    }

    /**
     * Whether the broker has delivered this message before.
     *
     * <p>True after a rollback or an unacknowledged delivery, so it is the signal to apply
     * idempotence in a handler with side effects.</p>
     *
     * @return {@code true} if the broker flagged the message as redelivered
     */
    public boolean isRedelivered() {
        return this.rawMessage != null && this.rawMessage.getRedelivered();
    }

    /**
     * Whether the broker reported a delivery count at all.
     *
     * <p>Check this before branching on {@code getDeliveryCount()}: an unsupported broker reports
     * {@code -1}, which any {@code >= n} comparison silently treats as a first delivery.</p>
     *
     * @return {@code true} when {@code getDeliveryCount()} carries a real number
     */
    public boolean isDeliveryCountSupported() {
        return this.deliveryCount >= 0;
    }
}
