package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Low level listener contract; the container acknowledges (or commits) after a normal return.
 * Analogous to Spring for Apache Kafka's {@code MessageListener}.
 */
@FunctionalInterface
public interface SolaceMessageListener {

    /**
     * Handle one message.
     *
     * @param message the received message
     * @throws Exception to signal failure. The container then either acknowledges anyway
     *                   ({@code ackOnError}), leaves the message unacknowledged, or rolls the
     *                   transaction back so the broker redelivers
     */
    void onMessage(BytesXMLMessage message) throws Exception;
}
