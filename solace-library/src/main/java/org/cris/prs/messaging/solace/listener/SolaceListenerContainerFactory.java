package org.cris.prs.messaging.solace.listener;

/** Creates listener containers for endpoints, like {@code KafkaListenerContainerFactory}. */
@FunctionalInterface
public interface SolaceListenerContainerFactory {

    /**
     * Build a container for an endpoint.
     *
     * @param endpoint the endpoint description, with its pattern defaults already applied
     * @return a container, not yet started
     */
    SolaceMessageListenerContainer createListenerContainer(SolaceListenerEndpoint endpoint);
}
