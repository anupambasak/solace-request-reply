package cris.prs.messaging.solace.observability;

import cris.prs.messaging.solace.requestreply.SolaceRequestReplyMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Publishes request-reply measurements to a Micrometer {@code MeterRegistry}.
 *
 * <p>Registered automatically when a {@code MeterRegistry} bean is present and
 * {@code solace.metrics.enabled} is not {@code false}. Every template built by
 * {@code ReplyingSolaceTemplateFactory} then reports through it, including additional reply
 * destinations declared by the application.</p>
 *
 * <p>Meters published, all tagged {@value SolaceMetricNames#TAG_TEMPLATE} with the template id and,
 * where a request destination is known, {@value SolaceMetricNames#TAG_DESTINATION}:</p>
 * <ul>
 *   <li>{@value SolaceMetricNames#REQUESTS_SENT} &mdash; counter of requests published;</li>
 *   <li>{@value SolaceMetricNames#REQUESTS_SEND_FAILED} &mdash; counter of publish failures;</li>
 *   <li>{@value SolaceMetricNames#REQUESTS_LATENCY} &mdash; timer of round-trip latency;</li>
 *   <li>{@value SolaceMetricNames#REQUESTS_TIMEOUTS} &mdash; counter of requests that timed out;</li>
 *   <li>{@value SolaceMetricNames#REPLIES_UNMATCHED} &mdash; counter of replies with no outstanding
 *       request.</li>
 * </ul>
 *
 * <p>The destination tag is the request topic, which an application controls. Publishing to
 * destinations built from unbounded values &mdash; an id per request, say &mdash; would produce
 * unbounded tag cardinality; that is a property of the destination naming, not of this class.</p>
 */
public class MicrometerSolaceRequestReplyMetrics implements SolaceRequestReplyMetrics {

    private final MeterRegistry meterRegistry;

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    /**
     * Create the collaborator.
     *
     * @param meterRegistry where meters are published
     */
    public MicrometerSolaceRequestReplyMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public void recordRequest(String templateId, String destination) {
        counter(SolaceMetricNames.REQUESTS_SENT, "Requests published through a ReplyingSolaceTemplate",
                templateId, destination).increment();
    }

    /** {@inheritDoc} */
    @Override
    public void recordSendFailure(String templateId, String destination) {
        counter(SolaceMetricNames.REQUESTS_SEND_FAILED, "Requests that could not be published",
                templateId, destination).increment();
    }

    /** {@inheritDoc} */
    @Override
    public void recordReply(String templateId, String destination, long latencyMillis) {
        if (latencyMillis < 0) {
            return;
        }
        this.timers.computeIfAbsent(templateId + '|' + destination, key -> Timer
                .builder(SolaceMetricNames.REQUESTS_LATENCY)
                .description("Request-reply round-trip time measured by the requester")
                .tag(SolaceMetricNames.TAG_TEMPLATE, templateId)
                .tag(SolaceMetricNames.TAG_DESTINATION, destination == null ? "unknown" : destination)
                .register(this.meterRegistry)).record(latencyMillis, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public void recordTimeout(String templateId, String destination) {
        counter(SolaceMetricNames.REQUESTS_TIMEOUTS, "Requests whose reply did not arrive in time",
                templateId, destination).increment();
    }

    /** {@inheritDoc} */
    @Override
    public void recordUnmatchedReply(String templateId) {
        counter(SolaceMetricNames.REPLIES_UNMATCHED, "Replies with no outstanding request",
                templateId, null).increment();
    }

    /**
     * Resolve a counter for one tag combination, creating and caching it on first use.
     *
     * @param name        the meter name
     * @param description the meter description
     * @param templateId  the template's id
     * @param destination the request destination, or {@code null} to omit the tag
     * @return the counter to increment
     */
    private Counter counter(String name, String description, String templateId, String destination) {
        return this.counters.computeIfAbsent(name + '|' + templateId + '|' + destination, key -> {
            Counter.Builder builder = Counter.builder(name)
                    .description(description)
                    .tag(SolaceMetricNames.TAG_TEMPLATE, templateId);
            if (destination != null) {
                builder.tag(SolaceMetricNames.TAG_DESTINATION, destination);
            }
            return builder.register(this.meterRegistry);
        });
    }
}
