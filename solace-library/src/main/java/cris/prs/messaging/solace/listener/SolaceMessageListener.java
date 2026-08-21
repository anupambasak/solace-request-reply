package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Low level listener contract; the container acknowledges (or commits) after a normal return.
 * Analogous to Spring for Apache Kafka's {@code MessageListener}.
 */
@FunctionalInterface
public interface SolaceMessageListener {

    void onMessage(BytesXMLMessage message) throws Exception;
}
