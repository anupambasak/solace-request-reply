package org.cris.prs.messaging.solace.observability;

import org.cris.prs.messaging.solace.core.SolaceSessionFactory;
import org.cris.prs.messaging.solace.core.SolaceSessionState;
import org.cris.prs.messaging.solace.listener.SolaceListenerEndpoint;
import org.cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import org.cris.prs.messaging.solace.listener.SolaceMessageListener;
import org.cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the two observability features without a broker: what the Micrometer collaborators publish,
 * and what makes the health indicator report DOWN.
 */
class SolaceObservabilityTest {

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        this.meterRegistry = new SimpleMeterRegistry();
    }

    @Nested
    @DisplayName("listener metrics")
    class ListenerMetrics {

        @Test
        @DisplayName("counts deliveries and times successful invocations separately")
        void countsAndTimes() {
            MicrometerSolaceListenerMetrics metrics =
                    new MicrometerSolaceListenerMetrics(SolaceObservabilityTest.this.meterRegistry);

            metrics.recordReceived("orders");
            metrics.recordReceived("orders");
            metrics.recordSuccess("orders", TimeUnit.MILLISECONDS.toNanos(5));

            assertEquals(2.0, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.LISTENER_RECEIVED)
                    .tag(SolaceMetricNames.TAG_LISTENER, "orders")
                    .counter().count());
            assertEquals(1L, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.LISTENER_PROCESSING)
                    .tag(SolaceMetricNames.TAG_RESULT, SolaceMetricNames.RESULT_SUCCESS)
                    .timer().count());
        }

        @Test
        @DisplayName("tags a failure with the exception type, so one bad message type is visible")
        void tagsFailures() {
            MicrometerSolaceListenerMetrics metrics =
                    new MicrometerSolaceListenerMetrics(SolaceObservabilityTest.this.meterRegistry);

            metrics.recordFailure("orders", 1_000L, new IllegalStateException("boom"));

            assertEquals(1L, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.LISTENER_PROCESSING)
                    .tag(SolaceMetricNames.TAG_RESULT, SolaceMetricNames.RESULT_FAILURE)
                    .tag(SolaceMetricNames.TAG_EXCEPTION, "IllegalStateException")
                    .timer().count());
        }
    }

    @Nested
    @DisplayName("request-reply metrics")
    class RequestReplyMetrics {

        @Test
        @DisplayName("records sends, latency and timeouts against the request destination")
        void recordsTheRoundTrip() {
            MicrometerSolaceRequestReplyMetrics metrics =
                    new MicrometerSolaceRequestReplyMetrics(SolaceObservabilityTest.this.meterRegistry);

            metrics.recordRequest("solaceReplyContainer", "pricing/quote");
            metrics.recordReply("solaceReplyContainer", "pricing/quote", 12L);
            metrics.recordTimeout("solaceReplyContainer", "pricing/quote");

            assertEquals(1.0, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.REQUESTS_SENT)
                    .tag(SolaceMetricNames.TAG_DESTINATION, "pricing/quote").counter().count());
            assertEquals(1L, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.REQUESTS_LATENCY).timer().count());
            assertEquals(1.0, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.REQUESTS_TIMEOUTS).counter().count());
        }

        @Test
        @DisplayName("ignores a negative latency, which means no reply has arrived yet")
        void ignoresNegativeLatency() {
            MicrometerSolaceRequestReplyMetrics metrics =
                    new MicrometerSolaceRequestReplyMetrics(SolaceObservabilityTest.this.meterRegistry);

            metrics.recordReply("solaceReplyContainer", "pricing/quote", -1L);

            assertTrue(SolaceObservabilityTest.this.meterRegistry
                    .find(SolaceMetricNames.REQUESTS_LATENCY).timers().isEmpty());
        }

        @Test
        @DisplayName("counts an unmatched reply without a destination tag")
        void countsUnmatchedReplies() {
            MicrometerSolaceRequestReplyMetrics metrics =
                    new MicrometerSolaceRequestReplyMetrics(SolaceObservabilityTest.this.meterRegistry);

            metrics.recordUnmatchedReply("solaceReplyContainer");

            assertEquals(1.0, SolaceObservabilityTest.this.meterRegistry
                    .get(SolaceMetricNames.REPLIES_UNMATCHED)
                    .tag(SolaceMetricNames.TAG_TEMPLATE, "solaceReplyContainer").counter().count());
        }
    }

    @Nested
    @DisplayName("session statistics")
    class SessionStatistics {

        @Test
        @DisplayName("turns a JCSMP StatType name into a Micrometer meter name")
        void meterNaming() {
            assertEquals("solace.session.total.msgs.sent",
                    SolaceSessionStatistics.meterName("TOTAL_MSGS_SENT"));
            assertEquals("solace.session.publisher.window.closed",
                    SolaceSessionStatistics.meterName("PUBLISHER_WINDOW_CLOSED"));
        }

        @Test
        @DisplayName("the curated default set covers throughput, trouble and back-pressure")
        void defaultsAreCurated() {
            assertTrue(SolaceSessionStatistics.DEFAULTS.contains("TOTAL_MSGS_SENT"));
            assertTrue(SolaceSessionStatistics.DEFAULTS.contains("RELIABLE_MSGS_RESENT"));
            assertTrue(SolaceSessionStatistics.DEFAULTS.contains("PUBLISHER_WINDOW_CLOSED"));
            assertTrue(SolaceSessionStatistics.DEFAULTS.size() < 25,
                    "the point of a curated set is that it is small");
        }
    }

    @Nested
    @DisplayName("health indicator")
    class HealthIndicatorTest {

        @Test
        @DisplayName("is UP when the session is connected and every container is running")
        void upWhenEverythingRuns() {
            Health health = indicator(true, true, true).health();

            assertEquals(Status.UP, health.getStatus());
            assertEquals("connected", health.getDetails().get("session"));
            assertFalse(health.getDetails().containsKey("stoppedContainers"));
        }

        @Test
        @DisplayName("is DOWN when a container is stopped, and names it")
        void downWhenAContainerIsStopped() {
            Health health = indicator(true, true, false).health();

            assertEquals(Status.DOWN, health.getStatus());
            assertEquals(List.of("stopped-one"), health.getDetails().get("stoppedContainers"));
        }

        @Test
        @DisplayName("stays UP with a stopped container when the requirement is relaxed")
        void relaxedRequirement() {
            Health health = indicator(false, true, false).health();

            assertEquals(Status.UP, health.getStatus());
            assertEquals(List.of("stopped-one"), health.getDetails().get("stoppedContainers"));
        }

        @Test
        @DisplayName("is DOWN when the session is gone, however healthy the containers look")
        void downWhenTheSessionIsGone() {
            Health health = indicator(false, SolaceSessionState.DOWN, true).health();

            assertEquals(Status.DOWN, health.getStatus());
            assertEquals("down", health.getDetails().get("session"));
        }

        @Test
        @DisplayName("reports reconnecting distinctly from down, and is DOWN for both")
        void reconnectingIsItsOwnState() {
            Health health = indicator(false, SolaceSessionState.RECONNECTING, true).health();

            assertEquals(Status.DOWN, health.getStatus());
            assertEquals("reconnecting", health.getDetails().get("session"));
        }

        @Test
        @DisplayName("a session that has never connected is UP, not a fault")
        void notConnectedIsUp() {
            Health health = indicator(false, SolaceSessionState.NOT_CONNECTED, true).health();

            assertEquals(Status.UP, health.getStatus());
            assertEquals("not-connected", health.getDetails().get("session"));
        }

        @SuppressWarnings("unchecked")
        @Test
        @DisplayName("reports each container's state")
        void reportsContainerState() {
            Health health = indicator(true, true, false).health();

            Map<String, String> containers =
                    (Map<String, String>) health.getDetails().get("containers");
            assertEquals("running", containers.get("running-one"));
            assertEquals("stopped", containers.get("stopped-one"));
        }

        private SolaceHealthIndicator indicator(boolean requireAllRunning, boolean sessionHealthy,
                boolean secondContainerRunning) {
            return indicator(requireAllRunning,
                    sessionHealthy ? SolaceSessionState.CONNECTED : SolaceSessionState.DOWN,
                    secondContainerRunning);
        }

        private SolaceHealthIndicator indicator(boolean requireAllRunning, SolaceSessionState state,
                boolean secondContainerRunning) {
            SolaceListenerEndpointRegistry registry = new SolaceListenerEndpointRegistry();
            registry.registerListenerContainer(endpoint("running-one"),
                    ignored -> new FakeContainer("running-one", true));
            registry.registerListenerContainer(endpoint(secondContainerRunning ? "running-two" : "stopped-one"),
                    ignored -> new FakeContainer(secondContainerRunning ? "running-two" : "stopped-one",
                            secondContainerRunning));
            return new SolaceHealthIndicator(new FakeSessionFactory(state), registry,
                    List.of(), requireAllRunning);
        }

        private SolaceListenerEndpoint endpoint(String id) {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setId(id);
            return endpoint;
        }
    }

    /** A container that only has to answer for its id and its running state. */
    private static final class FakeContainer implements SolaceMessageListenerContainer {

        private final String id;

        private final boolean running;

        private FakeContainer(String id, boolean running) {
            this.id = id;
            this.running = running;
        }

        @Override
        public String getListenerId() {
            return this.id;
        }

        @Override
        public void setupMessageListener(SolaceMessageListener listener) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isRunning() {
            return this.running;
        }

        @Override
        public boolean isAutoStartup() {
            return false;
        }
    }

    /** A session factory that only has to answer the health question. */
    private record FakeSessionFactory(SolaceSessionState state) implements SolaceSessionFactory {

        @Override
        public SolaceSessionState getSessionState() {
            return this.state;
        }

        @Override
        public JCSMPSession getSharedSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JCSMPSession createSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public XMLMessageProducer getSharedProducer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public XMLMessageProducer getProducer(JCSMPSession session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransactedSession createTransactedSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransactedSession createTransactedSession(JCSMPSession session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void closeSession(JCSMPSession session) {
        }
    }
}
