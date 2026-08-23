package cris.prs.messaging.solace.observability;

/**
 * The meter and tag names this library publishes.
 *
 * <p>Names follow Micrometer's convention of lowercase, dot-separated segments, so each backend
 * applies its own naming convention (Prometheus renders {@code solace.listener.processing} as
 * {@code solace_listener_processing_seconds}). They are constants rather than literals so a
 * dashboard, an alert rule and the code cannot drift apart.</p>
 */
public final class SolaceMetricNames {

    private SolaceMetricNames() {
    }

    // --- listener ------------------------------------------------------------------------

    /** Counter: messages delivered to a container, before the listener is invoked. */
    public static final String LISTENER_RECEIVED = "solace.listener.messages.received";

    /** Timer: listener invocations, tagged {@value #TAG_RESULT} and {@value #TAG_EXCEPTION}. */
    public static final String LISTENER_PROCESSING = "solace.listener.processing";

    /** Counter: settlement outcomes applied to failed messages, tagged {@value #TAG_OUTCOME}. */
    public static final String LISTENER_SETTLEMENT = "solace.listener.settlement";

    /** Counter: flow lifecycle events, tagged {@value #TAG_EVENT}. A rising RECONNECTING count is broker instability. */
    public static final String LISTENER_FLOW_EVENTS = "solace.listener.flow.events";

    /** Gauge: {@code 1} while a container is the active consumer, {@code 0} while standing by or degraded. */
    public static final String LISTENER_ACTIVE = "solace.listener.active";

    /** Gauge: {@code 1} while any of a container's flows is down or reconnecting. */
    public static final String LISTENER_DEGRADED = "solace.listener.degraded";

    /** Gauge: {@code 1} while a container is running, {@code 0} otherwise. */
    public static final String LISTENER_RUNNING = "solace.listener.running";

    /** Gauge: flows a container currently has bound. Below the configured concurrency means flows were lost. */
    public static final String LISTENER_FLOWS = "solace.listener.flows";

    // --- request-reply -------------------------------------------------------------------

    /** Counter: requests published. */
    public static final String REQUESTS_SENT = "solace.requests.sent";

    /** Counter: requests that could not be published. */
    public static final String REQUESTS_SEND_FAILED = "solace.requests.send.failed";

    /** Timer: round-trip latency, measured by the requester, recorded when a reply completes its future. */
    public static final String REQUESTS_LATENCY = "solace.requests.latency";

    /** Counter: requests whose reply did not arrive within the timeout. */
    public static final String REQUESTS_TIMEOUTS = "solace.requests.timeouts";

    /** Gauge: requests still awaiting a reply. Unbounded growth means replies are not being matched. */
    public static final String REQUESTS_PENDING = "solace.requests.pending";

    /** Counter: replies that arrived with no outstanding request. */
    public static final String REPLIES_UNMATCHED = "solace.replies.unmatched";

    // --- tags ----------------------------------------------------------------------------

    /** Tag: the listener container's id. */
    public static final String TAG_LISTENER = "listener";

    /** Tag: the request-reply template's id. */
    public static final String TAG_TEMPLATE = "template";

    /** Tag: the request destination. */
    public static final String TAG_DESTINATION = "destination";

    /** Tag: the {@code SettlementOutcome} applied &mdash; {@code ACCEPTED}, {@code FAILED}, {@code REJECTED} or {@code NONE}. */
    public static final String TAG_OUTCOME = "outcome";

    /** Tag: the {@code SolaceFlowEvent} name &mdash; {@code UP}, {@code DOWN}, {@code RECONNECTING}, and so on. */
    public static final String TAG_EVENT = "event";

    /** Tag: {@code success} or {@code failure}. */
    public static final String TAG_RESULT = "result";

    /** Tag: the simple name of the exception thrown, or {@code none}. */
    public static final String TAG_EXCEPTION = "exception";

    /** Tag value for a successful invocation. */
    public static final String RESULT_SUCCESS = "success";

    /** Tag value for a failed invocation. */
    public static final String RESULT_FAILURE = "failure";

    /** Tag value used for {@value #TAG_EXCEPTION} when nothing was thrown. */
    public static final String EXCEPTION_NONE = "none";
}
