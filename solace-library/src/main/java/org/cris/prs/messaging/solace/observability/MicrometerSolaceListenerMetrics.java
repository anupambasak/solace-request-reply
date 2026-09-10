package org.cris.prs.messaging.solace.observability;

import org.cris.prs.messaging.solace.listener.SolaceListenerMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Publishes listener measurements to a Micrometer {@code MeterRegistry}.
 *
 * <p>Registered automatically when a {@code MeterRegistry} bean is present and
 * {@code solace.metrics.enabled} is not {@code false}. Every container built by the auto-configured
 * container factory then reports through it.</p>
 *
 * <p>Meters published, all tagged {@value SolaceMetricNames#TAG_LISTENER} with the container id:</p>
 * <ul>
 *   <li>{@value SolaceMetricNames#LISTENER_RECEIVED} &mdash; counter of deliveries;</li>
 *   <li>{@value SolaceMetricNames#LISTENER_PROCESSING} &mdash; timer of listener invocations, also
 *       tagged {@value SolaceMetricNames#TAG_RESULT} and {@value SolaceMetricNames#TAG_EXCEPTION};</li>
 *   <li>{@value SolaceMetricNames#LISTENER_SETTLEMENT} &mdash; counter of settlement outcomes applied
 *       to failed messages, tagged {@value SolaceMetricNames#TAG_OUTCOME};</li>
 *   <li>{@value SolaceMetricNames#LISTENER_FLOW_EVENTS} &mdash; counter of flow lifecycle events,
 *       tagged {@value SolaceMetricNames#TAG_EVENT}.</li>
 * </ul>
 *
 * <p>Meters are resolved once per tag combination and cached, because the registry lookup is more
 * expensive than the increment and this runs on the message path. The number of combinations is
 * bounded by the number of containers times the number of distinct exception types, so cardinality
 * stays low in practice; a listener that throws a great many distinct exception types is the one case
 * to watch.</p>
 */
public class MicrometerSolaceListenerMetrics implements SolaceListenerMetrics {

    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> receivedCounters = new ConcurrentHashMap<>();

    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    private final Map<String, Counter> settlementCounters = new ConcurrentHashMap<>();

    private final Map<String, Counter> flowEventCounters = new ConcurrentHashMap<>();

    /**
     * Create the collaborator.
     *
     * @param meterRegistry where meters are published
     */
    public MicrometerSolaceListenerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public void recordReceived(String listenerId) {
        this.receivedCounters.computeIfAbsent(listenerId, id -> Counter
                .builder(SolaceMetricNames.LISTENER_RECEIVED)
                .description("Messages delivered to a Solace listener container")
                .tag(SolaceMetricNames.TAG_LISTENER, id)
                .register(this.meterRegistry)).increment();
    }

    /** {@inheritDoc} */
    @Override
    public void recordSuccess(String listenerId, long durationNanos) {
        timer(listenerId, SolaceMetricNames.RESULT_SUCCESS, SolaceMetricNames.EXCEPTION_NONE)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public void recordFailure(String listenerId, long durationNanos, Exception exception) {
        String type = exception != null ? exception.getClass().getSimpleName()
                : SolaceMetricNames.EXCEPTION_NONE;
        timer(listenerId, SolaceMetricNames.RESULT_FAILURE, type)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public void recordSettlement(String listenerId, String outcome) {
        this.settlementCounters.computeIfAbsent(listenerId + '|' + outcome, key -> Counter
                .builder(SolaceMetricNames.LISTENER_SETTLEMENT)
                .description("Settlement outcomes applied to failed messages")
                .tag(SolaceMetricNames.TAG_LISTENER, listenerId)
                .tag(SolaceMetricNames.TAG_OUTCOME, outcome)
                .register(this.meterRegistry)).increment();
    }

    /** {@inheritDoc} */
    @Override
    public void recordFlowEvent(String listenerId, String event) {
        this.flowEventCounters.computeIfAbsent(listenerId + '|' + event, key -> Counter
                .builder(SolaceMetricNames.LISTENER_FLOW_EVENTS)
                .description("Flow lifecycle events on a Solace listener container")
                .tag(SolaceMetricNames.TAG_LISTENER, listenerId)
                .tag(SolaceMetricNames.TAG_EVENT, event)
                .register(this.meterRegistry)).increment();
    }

    /**
     * Resolve the timer for one tag combination, creating and caching it on first use.
     *
     * @param listenerId the container's id
     * @param result     {@code success} or {@code failure}
     * @param exception  the exception's simple name, or {@code none}
     * @return the timer to record into
     */
    private Timer timer(String listenerId, String result, String exception) {
        return this.timers.computeIfAbsent(listenerId + '|' + result + '|' + exception, key -> Timer
                .builder(SolaceMetricNames.LISTENER_PROCESSING)
                .description("Time spent in a Solace listener method")
                .tag(SolaceMetricNames.TAG_LISTENER, listenerId)
                .tag(SolaceMetricNames.TAG_RESULT, result)
                .tag(SolaceMetricNames.TAG_EXCEPTION, exception)
                .register(this.meterRegistry));
    }
}
