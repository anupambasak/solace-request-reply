package org.cris.prs.messaging.solace.observability;

import org.cris.prs.messaging.solace.listener.DefaultSolaceMessageListenerContainer;
import org.cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import org.cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Registers the state gauges that cannot be recorded on the message path: whether each listener
 * container is running, how many flows it holds, and how many requests each request-reply template
 * is still waiting on.
 *
 * <p>Counters and timers are recorded as messages flow, by
 * {@link MicrometerSolaceListenerMetrics} and {@link MicrometerSolaceRequestReplyMetrics}. Gauges are
 * different: they sample live objects, so they must be registered once the objects exist.</p>
 *
 * <p>This is a {@link SmartLifecycle} in the highest phase rather than a Micrometer
 * {@code MeterBinder} for exactly that reason. A {@code MeterBinder} is bound when the
 * {@code MeterRegistry} bean is initialised, which can be before listener containers have been
 * registered &mdash; registration happens in the annotation post-processor's
 * {@code afterSingletonsInstantiated}. Starting last means every container is registered and started
 * before the gauges are created.</p>
 *
 * <p>Gauges hold weak references to their subject in Micrometer, so the container and template
 * references here are held for the life of this bean to keep the gauges alive. Registration is
 * idempotent: {@link #start()} skips containers it has already seen, so a lifecycle restart does not
 * duplicate meters.</p>
 */
@Slf4j
public class SolaceMetricsBinder implements SmartLifecycle {

    private final MeterRegistry meterRegistry;

    private final SolaceListenerEndpointRegistry endpointRegistry;

    private final Collection<ReplyingSolaceTemplate> replyingTemplates;

    private final SolaceSessionFactory sessionFactory;

    /** JCSMP {@code StatType} names to sample as session statistics. */
    private final List<String> sessionStatistics;

    /** Containers whose gauges are already registered, so a restart does not duplicate them. */
    private final Set<String> boundContainers = new HashSet<>();

    /** Templates whose gauges are already registered. */
    private final Set<String> boundTemplates = new HashSet<>();

    private volatile boolean running;

    /**
     * Create the binder.
     *
     * @param meterRegistry     where gauges are published
     * @param endpointRegistry  supplies the listener containers to sample
     * @param replyingTemplates every request-reply template in the context, including additional
     *                          reply destinations declared by the application; may be empty
     * @param sessionFactory    sampled for connection state and broker-side statistics
     * @param sessionStatistics JCSMP {@code StatType} names to publish as counters
     */
    public SolaceMetricsBinder(MeterRegistry meterRegistry,
            SolaceListenerEndpointRegistry endpointRegistry,
            Collection<ReplyingSolaceTemplate> replyingTemplates,
            SolaceSessionFactory sessionFactory, List<String> sessionStatistics) {
        this.meterRegistry = meterRegistry;
        this.endpointRegistry = endpointRegistry;
        this.replyingTemplates = replyingTemplates;
        this.sessionFactory = sessionFactory;
        this.sessionStatistics = sessionStatistics;
    }

    /**
     * Register a gauge for every container and template not already bound.
     *
     * <p>Runs after everything else has started, so nothing is missed.</p>
     */
    @Override
    public void start() {
        this.endpointRegistry.getListenerContainers().forEach(this::bindContainer);
        this.replyingTemplates.forEach(this::bindTemplate);
        bindSession();
        this.running = true;
        log.debug("Registered Solace gauges for {} container(s) and {} request-reply template(s)",
                this.boundContainers.size(), this.boundTemplates.size());
    }

    /**
     * Bind the gauges for one container.
     *
     * @param container the container to sample
     */
    private void bindContainer(SolaceMessageListenerContainer container) {
        if (!this.boundContainers.add(container.getListenerId())) {
            return;
        }
        Gauge.builder(SolaceMetricNames.LISTENER_RUNNING, container,
                        candidate -> candidate.isRunning() ? 1 : 0)
                .description("1 while a Solace listener container is running, 0 otherwise")
                .tag(SolaceMetricNames.TAG_LISTENER, container.getListenerId())
                .strongReference(true)
                .register(this.meterRegistry);
        if (container instanceof DefaultSolaceMessageListenerContainer defaultContainer) {
            Gauge.builder(SolaceMetricNames.LISTENER_FLOWS, defaultContainer,
                            DefaultSolaceMessageListenerContainer::getActiveFlowCount)
                    .description("Flows a Solace listener container currently has bound")
                    .tag(SolaceMetricNames.TAG_LISTENER, container.getListenerId())
                    .strongReference(true)
                    .register(this.meterRegistry);
            Gauge.builder(SolaceMetricNames.LISTENER_ACTIVE, defaultContainer,
                            candidate -> candidate.isActive() ? 1 : 0)
                    .description("1 while the container is the active consumer, 0 while standing by")
                    .tag(SolaceMetricNames.TAG_LISTENER, container.getListenerId())
                    .strongReference(true)
                    .register(this.meterRegistry);
            Gauge.builder(SolaceMetricNames.LISTENER_DEGRADED, defaultContainer,
                            candidate -> candidate.isDegraded() ? 1 : 0)
                    .description("1 while any of the container's flows is down or reconnecting")
                    .tag(SolaceMetricNames.TAG_LISTENER, container.getListenerId())
                    .strongReference(true)
                    .register(this.meterRegistry);
        }
    }

    /**
     * Bind the gauge for one request-reply template.
     *
     * @param template the template to sample
     */
    private void bindTemplate(ReplyingSolaceTemplate template) {
        if (!this.boundTemplates.add(template.getId())) {
            return;
        }
        Gauge.builder(SolaceMetricNames.REQUESTS_PENDING, template,
                        ReplyingSolaceTemplate::getPendingCount)
                .description("Requests still awaiting a reply")
                .tag(SolaceMetricNames.TAG_TEMPLATE, template.getId())
                .strongReference(true)
                .register(this.meterRegistry);
    }

    /**
     * Bind the session state gauge and one counter per sampled broker statistic.
     *
     * <p>The statistics are cumulative counters that JCSMP owns, so they are registered as
     * {@code FunctionCounter}s reading through to the session rather than counters this library
     * increments. Each is sampled lazily: a factory with no session yet simply reports zero, and
     * nothing here ever opens a connection.</p>
     */
    private void bindSession() {
        if (!this.boundTemplates.add("__session__")) {
            return;
        }
        Gauge.builder(SolaceMetricNames.SESSION_STATE, this.sessionFactory,
                        factory -> switch (factory.getSessionState()) {
                            case CONNECTED -> 2;
                            case RECONNECTING -> 1;
                            case DOWN -> 0;
                            case NOT_CONNECTED -> -1;
                        })
                .description("Solace session state: 2 connected, 1 reconnecting, 0 down, -1 not connected")
                .strongReference(true)
                .register(this.meterRegistry);

        for (String statistic : this.sessionStatistics) {
            FunctionCounter.builder(SolaceSessionStatistics.meterName(statistic), this.sessionFactory,
                            factory -> factory.getSessionStatistics(List.of(statistic))
                                    .getOrDefault(statistic, 0L))
                    .description("JCSMP session statistic " + statistic)
                    .register(this.meterRegistry);
        }
        log.debug("Registered {} Solace session statistic(s)", this.sessionStatistics.size());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Gauges are left registered: a stopped container reporting {@code 0} is more useful than a
     * gauge that disappears, which most backends render as a gap rather than a zero.</p>
     */
    @Override
    public void stop() {
        this.running = false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRunning() {
        return this.running;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code Integer.MAX_VALUE}, so this starts after every container and template
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
