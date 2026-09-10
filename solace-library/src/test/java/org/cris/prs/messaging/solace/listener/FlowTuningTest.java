package org.cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowEvent;
import org.cris.prs.messaging.solace.core.SolaceFlowEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the two properties of flow tuning and flow events that would otherwise only show up against
 * a live broker: that an untouched tuning block changes nothing, and that active flow indication is
 * derived from the endpoint's access type.
 */
class FlowTuningTest {

    @Nested
    @DisplayName("flow tuning")
    class Tuning {

        @Test
        @DisplayName("an untouched block leaves every JCSMP default alone")
        void untouchedBlockIsANoOp() {
            ConsumerFlowProperties untouched = new ConsumerFlowProperties();
            ConsumerFlowProperties applied = new ConsumerFlowProperties();

            new ContainerProperties.Flow().applyTo(applied, false);

            assertEquals(untouched.getTransportWindowSize(), applied.getTransportWindowSize());
            assertEquals(untouched.getAckThreshold(), applied.getAckThreshold());
            assertEquals(untouched.getAckTimerInMsecs(), applied.getAckTimerInMsecs());
            assertEquals(untouched.getReconnectTries(), applied.getReconnectTries());
        }

        @Test
        @DisplayName("applies only the values that were set, converting durations to millis")
        void appliesWhatWasSet() {
            ContainerProperties.Flow tuning = new ContainerProperties.Flow();
            tuning.setTransportWindowSize(512);
            tuning.setAckThreshold(80);
            tuning.setAckTimer(Duration.ofMillis(250));
            tuning.setReconnectRetryInterval(Duration.ofSeconds(3));

            ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
            tuning.applyTo(flowProperties, false);

            assertEquals(512, flowProperties.getTransportWindowSize());
            assertEquals(80, flowProperties.getAckThreshold());
            assertEquals(250, flowProperties.getAckTimerInMsecs());
            assertEquals(3000, flowProperties.getReconnectRetryIntervalInMsecs());
        }

        @Test
        @DisplayName("derives active flow indication from the access type when it is unset")
        void derivesActiveFlowIndication() {
            ConsumerFlowProperties exclusive = new ConsumerFlowProperties();
            new ContainerProperties.Flow().applyTo(exclusive, true);
            assertTrue(exclusive.isActiveFlowIndication());

            ConsumerFlowProperties shared = new ConsumerFlowProperties();
            new ContainerProperties.Flow().applyTo(shared, false);
            assertFalse(shared.isActiveFlowIndication());
        }

        @Test
        @DisplayName("noLocal is only applied when set, so the default stays JCSMP's")
        void noLocalIsOptional() {
            ConsumerFlowProperties untouched = new ConsumerFlowProperties();
            ConsumerFlowProperties applied = new ConsumerFlowProperties();
            new ContainerProperties.Flow().applyTo(applied, false);
            assertEquals(untouched.isNoLocal(), applied.isNoLocal());

            ContainerProperties.Flow tuning = new ContainerProperties.Flow();
            tuning.setNoLocal(true);
            ConsumerFlowProperties suppressed = new ConsumerFlowProperties();
            tuning.applyTo(suppressed, false);
            assertTrue(suppressed.isNoLocal());
        }

        @Test
        @DisplayName("an explicit setting overrides the derivation in both directions")
        void explicitSettingWins() {
            ContainerProperties.Flow off = new ContainerProperties.Flow();
            off.setActiveFlowIndication(false);
            ConsumerFlowProperties exclusive = new ConsumerFlowProperties();
            off.applyTo(exclusive, true);
            assertFalse(exclusive.isActiveFlowIndication());

            ContainerProperties.Flow on = new ContainerProperties.Flow();
            on.setActiveFlowIndication(true);
            ConsumerFlowProperties shared = new ConsumerFlowProperties();
            on.applyTo(shared, false);
            assertTrue(shared.isActiveFlowIndication());
        }
    }

    @Nested
    @DisplayName("flow events")
    class Events {

        @Test
        @DisplayName("maps every JCSMP event, and anything unrecognised to UNKNOWN")
        void mapsEveryEvent() {
            assertEquals(SolaceFlowEvent.UP, SolaceFlowEvent.from(FlowEvent.FLOW_UP));
            assertEquals(SolaceFlowEvent.DOWN, SolaceFlowEvent.from(FlowEvent.FLOW_DOWN));
            assertEquals(SolaceFlowEvent.RECONNECTING, SolaceFlowEvent.from(FlowEvent.FLOW_RECONNECTING));
            assertEquals(SolaceFlowEvent.RECONNECTED, SolaceFlowEvent.from(FlowEvent.FLOW_RECONNECTED));
            assertEquals(SolaceFlowEvent.ACTIVE, SolaceFlowEvent.from(FlowEvent.FLOW_ACTIVE));
            assertEquals(SolaceFlowEvent.INACTIVE, SolaceFlowEvent.from(FlowEvent.FLOW_INACTIVE));
            assertEquals(SolaceFlowEvent.UNKNOWN, SolaceFlowEvent.from(null));
        }

        @Test
        @DisplayName("only DOWN and RECONNECTING count as degraded — a standby flow is healthy")
        void standbyIsNotDegraded() {
            assertTrue(SolaceFlowEvent.DOWN.isDegraded());
            assertTrue(SolaceFlowEvent.RECONNECTING.isDegraded());
            assertFalse(SolaceFlowEvent.INACTIVE.isDegraded());
            assertFalse(SolaceFlowEvent.UP.isDegraded());
            assertFalse(SolaceFlowEvent.ACTIVE.isDegraded());
        }

        @Test
        @DisplayName("carries everything needed to act on an event")
        void eventArgsCarryContext() {
            Exception cause = new IllegalStateException("bind rejected");
            SolaceFlowEventArgs args = new SolaceFlowEventArgs("orders", 2, "orders.workers",
                    SolaceFlowEvent.DOWN, "Queue deleted", cause, 503);

            assertEquals("orders", args.getListenerId());
            assertEquals(2, args.getFlowIndex());
            assertEquals("orders.workers", args.getEndpoint());
            assertEquals(SolaceFlowEvent.DOWN, args.getEvent());
            assertEquals("Queue deleted", args.getInfo());
            assertEquals(cause, args.getException());
            assertEquals(503, args.getResponseCode());
        }
    }
}
