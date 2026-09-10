package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.SessionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the session-state judgements that matter to a readiness probe: a reconnecting session is
 * not healthy, and a session that has never connected is not a fault.
 */
class SolaceSessionStateTest {

    @Nested
    @DisplayName("session events")
    class Events {

        @Test
        @DisplayName("maps every JCSMP event, and anything unrecognised to UNKNOWN")
        void mapsEveryEvent() {
            assertEquals(SolaceSessionEvent.RECONNECTING,
                    SolaceSessionEvent.from(SessionEvent.RECONNECTING));
            assertEquals(SolaceSessionEvent.RECONNECTED,
                    SolaceSessionEvent.from(SessionEvent.RECONNECTED));
            assertEquals(SolaceSessionEvent.DOWN,
                    SolaceSessionEvent.from(SessionEvent.DOWN_ERROR));
            assertEquals(SolaceSessionEvent.SUBSCRIPTION_ERROR,
                    SolaceSessionEvent.from(SessionEvent.SUBSCRIPTION_ERROR));
            assertEquals(SolaceSessionEvent.VIRTUAL_ROUTER_NAME_CHANGED,
                    SolaceSessionEvent.from(SessionEvent.VIRTUAL_ROUTER_NAME_CHANGED));
            assertEquals(SolaceSessionEvent.INCOMPLETE_LARGE_MESSAGE,
                    SolaceSessionEvent.from(SessionEvent.INCOMPLETE_LARGE_MESSAGE_RECVD));
            assertEquals(SolaceSessionEvent.UNKNOWN_TRANSACTED_SESSION,
                    SolaceSessionEvent.from(SessionEvent.UNKNOWN_TRANSACTED_SESSION_NAME));
            assertEquals(SolaceSessionEvent.UNKNOWN, SolaceSessionEvent.from(null));
        }

        @Test
        @DisplayName("carries the state the event moved the session into")
        void argsCarryState() {
            Exception cause = new IllegalStateException("connection reset");
            SolaceSessionEventArgs args = new SolaceSessionEventArgs(SolaceSessionEvent.RECONNECTING,
                    SolaceSessionState.RECONNECTING, "retrying", cause, 0);

            assertEquals(SolaceSessionEvent.RECONNECTING, args.getEvent());
            assertEquals(SolaceSessionState.RECONNECTING, args.getState());
            assertEquals("retrying", args.getInfo());
            assertEquals(cause, args.getException());
        }
    }

    @Nested
    @DisplayName("session state")
    class State {

        @Test
        @DisplayName("a reconnecting session is not healthy — nothing is flowing while it retries")
        void reconnectingIsNotHealthy() {
            assertFalse(SolaceSessionState.RECONNECTING.isHealthy());
            assertFalse(SolaceSessionState.DOWN.isHealthy());
        }

        @Test
        @DisplayName("a session that has never connected is not a fault")
        void notConnectedIsHealthy() {
            assertTrue(SolaceSessionState.NOT_CONNECTED.isHealthy());
            assertTrue(SolaceSessionState.CONNECTED.isHealthy());
        }

        @Test
        @DisplayName("the default factory contract reports healthy, so a custom factory still compiles")
        void defaultContractIsHealthy() {
            SolaceSessionFactory minimal = new SolaceSessionFactory() {
                @Override
                public com.solacesystems.jcsmp.JCSMPSession getSharedSession() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.solacesystems.jcsmp.JCSMPSession createSession() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.solacesystems.jcsmp.XMLMessageProducer getSharedProducer() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.solacesystems.jcsmp.XMLMessageProducer getProducer(
                        com.solacesystems.jcsmp.JCSMPSession session) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.solacesystems.jcsmp.transaction.TransactedSession createTransactedSession() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.solacesystems.jcsmp.transaction.TransactedSession createTransactedSession(
                        com.solacesystems.jcsmp.JCSMPSession session) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void closeSession(com.solacesystems.jcsmp.JCSMPSession session) {
                }
            };

            assertEquals(SolaceSessionState.CONNECTED, minimal.getSessionState());
            assertTrue(minimal.isHealthy());
            assertTrue(minimal.getSessionStatistics(java.util.List.of("TOTAL_MSGS_SENT")).isEmpty());
        }
    }
}
