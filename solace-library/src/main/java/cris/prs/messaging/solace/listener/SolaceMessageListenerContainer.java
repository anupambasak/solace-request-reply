package cris.prs.messaging.solace.listener;

import org.springframework.context.SmartLifecycle;

/** A running listener, analogous to Spring for Apache Kafka's {@code MessageListenerContainer}. */
public interface SolaceMessageListenerContainer extends SmartLifecycle {

    /**
     * This container's identity.
     *
     * @return the container id, as used in logs and to look it up in the
     *         {@code SolaceListenerEndpointRegistry}
     */
    String getListenerId();

    /**
     * Set the listener to dispatch received messages to.
     *
     * <p>Must be called before {@code start()}.</p>
     *
     * @param listener the listener to invoke
     */
    void setupMessageListener(SolaceMessageListener listener);
}
