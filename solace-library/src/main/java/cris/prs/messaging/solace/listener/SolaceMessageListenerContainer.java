package cris.prs.messaging.solace.listener;

import org.springframework.context.SmartLifecycle;

/** A running listener, analogous to Spring for Apache Kafka's {@code MessageListenerContainer}. */
public interface SolaceMessageListenerContainer extends SmartLifecycle {

    String getListenerId();

    void setupMessageListener(SolaceMessageListener listener);
}
