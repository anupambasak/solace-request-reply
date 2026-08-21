package cris.prs.messaging.solace.listener;

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

    public SolaceMessageListenerContainer getListenerContainer(String id) {
        return this.containers.get(id);
    }

    public Set<String> getListenerContainerIds() {
        return this.containers.keySet();
    }

    public Collection<SolaceMessageListenerContainer> getListenerContainers() {
        return this.containers.values();
    }

    @Override
    public void start() {
        this.containers.values().stream()
                .filter(SolaceMessageListenerContainer::isAutoStartup)
                .sorted(Comparator.comparingInt(SmartLifecycle::getPhase))
                .forEach(SolaceMessageListenerContainer::start);
        this.running = true;
    }

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

    @Override
    public void destroy() {
        stop();
        this.containers.clear();
    }
}
