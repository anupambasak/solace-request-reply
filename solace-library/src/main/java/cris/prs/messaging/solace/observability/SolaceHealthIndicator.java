package cris.prs.messaging.solace.observability;

import cris.prs.messaging.solace.core.SolaceSessionFactory;
import cris.prs.messaging.solace.listener.DefaultSolaceMessageListenerContainer;
import cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reports Solace connectivity and listener state to Spring Boot Actuator, under
 * {@code /actuator/health/solace}.
 *
 * <p>Registered automatically when Actuator is on the classpath and {@code solace.health.enabled} is
 * not {@code false}. It reads state that is already held in memory &mdash; it never contacts the
 * broker &mdash; so it is cheap enough for a readiness probe on a short interval.</p>
 *
 * <h2>What makes it DOWN</h2>
 * <ul>
 *   <li>the session factory reports its connection is gone; or</li>
 *   <li>{@code solace.health.require-all-containers-running} is {@code true} (the default) and a
 *       registered listener container is not running.</li>
 * </ul>
 *
 * <p>Set {@code require-all-containers-running} to {@code false} for an application that starts
 * containers by hand, or declares listeners with {@code autoStartup = "false"} &mdash; a container
 * that is deliberately idle is not a fault, and reporting it as one would keep the pod out of the
 * load balancer forever.</p>
 *
 * <h2>Details reported</h2>
 * <ul>
 *   <li>{@code session} &mdash; {@code connected} or {@code disconnected};</li>
 *   <li>{@code containers} &mdash; each container id mapped to {@code running} or {@code stopped},
 *       with its resolved endpoint name where one is available;</li>
 *   <li>{@code stoppedContainers} &mdash; only when some are stopped, so the reason for a DOWN is
 *       the first thing visible;</li>
 *   <li>{@code pendingRequests} &mdash; outstanding requests per request-reply template.</li>
 * </ul>
 */
public class SolaceHealthIndicator implements HealthIndicator {

    private final SolaceSessionFactory sessionFactory;

    private final SolaceListenerEndpointRegistry endpointRegistry;

    private final Collection<ReplyingSolaceTemplate> replyingTemplates;

    private final boolean requireAllContainersRunning;

    /**
     * Create the indicator.
     *
     * @param sessionFactory              asked whether its connection is usable
     * @param endpointRegistry            supplies the listener containers to report on
     * @param replyingTemplates           every request-reply template in the context; may be empty
     * @param requireAllContainersRunning whether a stopped container makes the application DOWN
     */
    public SolaceHealthIndicator(SolaceSessionFactory sessionFactory,
            SolaceListenerEndpointRegistry endpointRegistry,
            Collection<ReplyingSolaceTemplate> replyingTemplates,
            boolean requireAllContainersRunning) {
        this.sessionFactory = sessionFactory;
        this.endpointRegistry = endpointRegistry;
        this.replyingTemplates = replyingTemplates;
        this.requireAllContainersRunning = requireAllContainersRunning;
    }

    /**
     * {@inheritDoc}
     *
     * @return UP, or DOWN with the offending detail, per the rules in the class documentation
     */
    @Override
    public Health health() {
        boolean sessionHealthy = this.sessionFactory.isHealthy();

        Map<String, String> containers = new TreeMap<>();
        this.endpointRegistry.getListenerContainers()
                .forEach(container -> containers.put(container.getListenerId(), describe(container)));

        List<String> stopped = this.endpointRegistry.getListenerContainers().stream()
                .filter(container -> !container.isRunning())
                .map(SolaceMessageListenerContainer::getListenerId)
                .sorted()
                .toList();

        Map<String, Integer> pending = new TreeMap<>();
        this.replyingTemplates.forEach(template ->
                pending.put(template.getId(), template.getPendingCount()));

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("session", sessionHealthy ? "connected" : "disconnected");
        details.put("containers", containers);
        if (!stopped.isEmpty()) {
            details.put("stoppedContainers", stopped);
        }
        if (!pending.isEmpty()) {
            details.put("pendingRequests", pending);
        }

        boolean up = sessionHealthy && (!this.requireAllContainersRunning || stopped.isEmpty());
        Health.Builder builder = up ? Health.up() : Health.down();
        return builder.withDetails(details).build();
    }

    /**
     * Describe one container: its state, and the endpoint it resolved to once started.
     *
     * @param container the container to describe
     * @return {@code running (endpoint)}, or {@code stopped}
     */
    private String describe(SolaceMessageListenerContainer container) {
        String state = container.isRunning() ? "running" : "stopped";
        if (container instanceof DefaultSolaceMessageListenerContainer defaultContainer) {
            String endpoint = defaultContainer.getResolvedQueueName();
            if (endpoint != null) {
                return state + " (" + endpoint + ")";
            }
        }
        return state;
    }
}
