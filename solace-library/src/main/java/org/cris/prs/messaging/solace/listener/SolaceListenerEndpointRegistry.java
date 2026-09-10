package org.cris.prs.messaging.solace.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds every listener container created from a {@code @SolaceListener} or registered
 * programmatically, and drives their lifecycle &mdash; the counterpart of
 * {@code KafkaListenerEndpointRegistry}.
 */
@Slf4j
public class SolaceListenerEndpointRegistry implements SmartLifecycle, DisposableBean {

    private final Map<String, SolaceMessageListenerContainer> containers = new ConcurrentHashMap<>();

    private volatile boolean running;

    /** Create an empty registry. Registered as an infrastructure bean by {@code @EnableSolace}. */
    public SolaceListenerEndpointRegistry() {
    }

    /**
     * Create and register a container for an endpoint.
     *
     * <p>If the registry is already running, an auto-startup container is started immediately, which
     * is what allows listeners to be added after the context has refreshed.</p>
     *
     * @param endpoint the endpoint description
     * @param factory  the factory to build the container with
     * @return the registered container
     * @throws IllegalStateException if the id is blank or already registered
     */
    public SolaceMessageListenerContainer registerListenerContainer(SolaceListenerEndpoint endpoint,
            SolaceListenerContainerFactory factory) {
        Assert.hasText(endpoint.getId(), "Endpoint id must not be empty");
        Assert.state(!this.containers.containsKey(endpoint.getId()),
                "Another Solace listener container is already registered with id '" + endpoint.getId() + "'");
        SolaceMessageListenerContainer container = factory.createListenerContainer(endpoint);
        this.containers.put(endpoint.getId(), container);
        if (this.running && container.isAutoStartup()) {
            container.start();
        }
        return container;
    }

    /**
     * Look a container up, for starting or stopping a listener at runtime.
     *
     * @param id the container id, as given by {@code @SolaceListener(id = ...)}
     * @return the container, or {@code null} if no listener is registered under that id
     */
    public SolaceMessageListenerContainer getListenerContainer(String id) {
        return this.containers.get(id);
    }

    /**
     * The ids of every registered container.
     *
     * @return the registered ids
     */
    public Set<String> getListenerContainerIds() {
        return this.containers.keySet();
    }

    /**
     * Every registered container.
     *
     * @return the registered containers
     */
    public Collection<SolaceMessageListenerContainer> getListenerContainers() {
        return this.containers.values();
    }

    /** Start every auto-startup container, in lifecycle phase order. */
    @Override
    public void start() {
        this.containers.values().stream()
                .filter(SolaceMessageListenerContainer::isAutoStartup)
                .sorted(Comparator.comparingInt(SmartLifecycle::getPhase))
                .forEach(SolaceMessageListenerContainer::start);
        this.running = true;
    }

    /** Stop every container, logging rather than propagating individual failures. */
    @Override
    public void stop() {
        this.running = false;
        this.containers.values().forEach(container -> {
            try {
                container.stop();
            }
            catch (Exception ex) {
                log.warn("Error stopping Solace listener container '{}'", container.getListenerId(), ex);
            }
        });
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    /** Stop every container and forget them. */
    @Override
    public void destroy() {
        stop();
        this.containers.clear();
    }
}
