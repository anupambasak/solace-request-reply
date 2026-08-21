package cris.prs.messaging.solace.core;

/**
 * Well-known header names used by the Solace messaging abstraction.
 *
 * <p>Headers whose name starts with {@link #PREFIX} are mapped onto native Solace message
 * fields where an equivalent exists; every other header is carried in the message's
 * {@code SDTMap} user property map.</p>
 */
public final class SolaceHeaders {

    private SolaceHeaders() {
    }

    /** Prefix reserved for framework headers mapped to native Solace message fields. */
    public static final String PREFIX = "solace_";

    /** Native {@code XMLMessage.getCorrelationId()}. */
    public static final String CORRELATION_ID = PREFIX + "correlationId";

    /** Native {@code XMLMessage.getReplyTo()}, exposed as a topic/queue name {@code String}. */
    public static final String REPLY_TO = PREFIX + "replyTo";

    /** Destination the message was received on. */
    public static final String DESTINATION = PREFIX + "destination";

    /** Native {@code XMLMessage.getApplicationMessageId()}. */
    public static final String APPLICATION_MESSAGE_ID = PREFIX + "applicationMessageId";

    /** Native {@code XMLMessage.getSenderTimestamp()}. */
    public static final String SENDER_TIMESTAMP = PREFIX + "senderTimestamp";

    /** Native {@code XMLMessage.getRedelivered()}. */
    public static final String REDELIVERED = PREFIX + "redelivered";

    /** Native {@code XMLMessage.getTimeToLive()} in milliseconds. */
    public static final String TIME_TO_LIVE = PREFIX + "timeToLive";

    /** Native {@code XMLMessage.getPriority()}. */
    public static final String PRIORITY = PREFIX + "priority";

    /** Overrides the destination a message is published to, analogous to Kafka's topic header. */
    public static final String TARGET_DESTINATION = PREFIX + "targetDestination";

    /** The raw {@code BytesXMLMessage}, added to inbound Spring messages. */
    public static final String RAW_MESSAGE = PREFIX + "rawMessage";

    /**
     * Identifier of the application instance (pod/host) that produced the request. Carried as an
     * SDT user property so that it can also be used in broker side selectors.
     */
    public static final String INSTANCE_ID = "instanceId";

    /** Millisecond epoch at which the request was published, used for latency reporting. */
    public static final String REQUEST_SEND_TIME = "requestSendTime";
}
