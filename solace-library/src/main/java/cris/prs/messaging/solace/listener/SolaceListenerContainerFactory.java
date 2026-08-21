package cris.prs.messaging.solace.listener;

/** Creates listener containers for endpoints, like {@code KafkaListenerContainerFactory}. */
@FunctionalInterface
public interface SolaceListenerContainerFactory {

    SolaceMessageListenerContainer createListenerContainer(SolaceListenerEndpoint endpoint);
}
