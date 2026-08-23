package cris.prs.messaging.solace.observability;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;
import cris.prs.messaging.solace.core.SettlementOutcome;
import cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the two properties of settlement that are easy to get wrong: which outcomes have to be
 * negotiated on the flow before they can be sent, and the fact that an error handler written as a
 * lambda still defers to the container.
 */
class SettlementOutcomeTest {

    @Test
    @DisplayName("maps to the JCSMP outcome, except NONE which sends nothing")
    void mapsToJcsmp() {
        assertEquals(XMLMessage.Outcome.ACCEPTED, SettlementOutcome.ACCEPTED.jcsmpOutcome());
        assertEquals(XMLMessage.Outcome.FAILED, SettlementOutcome.FAILED.jcsmpOutcome());
        assertEquals(XMLMessage.Outcome.REJECTED, SettlementOutcome.REJECTED.jcsmpOutcome());
        assertNull(SettlementOutcome.NONE.jcsmpOutcome());
    }

    @Test
    @DisplayName("only the negative outcomes must be negotiated at bind time")
    void onlyNegativeOutcomesNeedNegotiation() {
        assertTrue(SettlementOutcome.FAILED.requiresNegotiation());
        assertTrue(SettlementOutcome.REJECTED.requiresNegotiation());
        assertFalse(SettlementOutcome.ACCEPTED.requiresNegotiation());
        assertFalse(SettlementOutcome.NONE.requiresNegotiation());
    }

    @Test
    @DisplayName("an error handler written as a lambda leaves the outcome to the container")
    void lambdaHandlerDefersToTheContainer() {
        SolaceListenerErrorHandler handler = (message, exception) -> {
        };

        assertNull(handler.resolveOutcome(null, new IllegalStateException("boom")));
    }

    @Test
    @DisplayName("a handler can decide per failure, which is the point of the SPI")
    void handlerCanDecidePerFailure() {
        SolaceListenerErrorHandler handler = new SolaceListenerErrorHandler() {
            @Override
            public void handleError(BytesXMLMessage message, Exception exception) {
            }

            @Override
            public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
                return exception instanceof IllegalArgumentException
                        ? SettlementOutcome.REJECTED       // will never parse; do not retry
                        : SettlementOutcome.FAILED;        // transient; hand it back
            }
        };

        assertEquals(SettlementOutcome.REJECTED,
                handler.resolveOutcome(null, new IllegalArgumentException("bad payload")));
        assertEquals(SettlementOutcome.FAILED,
                handler.resolveOutcome(null, new IllegalStateException("downstream timeout")));
    }
}
