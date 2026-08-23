package cris.prs.messaging.solace.observability;

import java.util.List;

/**
 * The broker-side session statistics sampled by default.
 *
 * <p>JCSMP keeps around seventy counters on a session. Publishing all of them would bury the useful
 * ones and multiply the time series for no benefit, so this is a curated set chosen for what it tells
 * an operator that nothing else does &mdash; retransmits, discards, acknowledgement timeouts and
 * window stalls never surface in application metrics or in the log.</p>
 *
 * <p>Override the list entirely with {@code solace.metrics.session-statistics}, naming any JCSMP
 * {@code StatType} constant. An unrecognised name is logged and skipped rather than failing
 * startup.</p>
 */
public final class SolaceSessionStatistics {

    private SolaceSessionStatistics() {
    }

    /**
     * The default list.
     *
     * <p>Grouped by the question each answers:</p>
     * <ul>
     *   <li><b>throughput</b> &mdash; total messages and bytes each way, and confirmed publishes;</li>
     *   <li><b>trouble</b> &mdash; retransmits, discards, rejections and acknowledgement timeouts;</li>
     *   <li><b>back-pressure</b> &mdash; how often a publisher or subscriber window closed, which is
     *       the clearest signal that a transport window needs tuning;</li>
     *   <li><b>connection churn</b> &mdash; total connection attempts, which rises on every
     *       reconnect.</li>
     * </ul>
     */
    public static final List<String> DEFAULTS = List.of(
            // throughput
            "TOTAL_MSGS_SENT",
            "TOTAL_MSGS_RECVED",
            "TOTAL_BYTES_SENT",
            "TOTAL_BYTES_RECVED",
            "RELIABLE_MSGS_SENT_CONFIRMED",
            "RELIABLE_MSGS_RECVED_ACKED",
            // trouble
            "RELIABLE_MSGS_RESENT",
            "RELIABLE_MSGS_DISCARDED_DUPLICATES",
            "RELIABLE_MSGS_DISCARDED_OUTOFORDER",
            "MESSAGES_DISCARDED_INTERNAL",
            "MESSAGES_REJECTED_BY_APPLIANCE",
            "TOTAL_ACK_TIMEOUT",
            "TOTAL_ERROR_RESPONSE_CALLBACKS",
            // back-pressure
            "PUBLISHER_WINDOW_CLOSED",
            "SUBSCRIBER_FLOW_WINDOW_CLOSED",
            // connection churn
            "TOTAL_CONNECTION_ATTEMPTS");

    /**
     * Turn a {@code StatType} name into a meter name.
     *
     * <p>{@code TOTAL_MSGS_SENT} becomes {@code solace.session.total.msgs.sent}, which is Micrometer's
     * naming convention and lets each backend apply its own.</p>
     *
     * @param statType the JCSMP constant name
     * @return the meter name
     */
    public static String meterName(String statType) {
        return SolaceMetricNames.SESSION_STATISTIC_PREFIX
                + statType.toLowerCase().replace('_', '.');
    }
}
