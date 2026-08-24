package cris.prs.messaging.solace.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing and value semantics for a replay start point, which reaches the library as a String. */
class ReplayStartPointTest {

    @Test
    @DisplayName("BEGINNING parses in any case")
    void parsesBeginning() {
        assertTrue(ReplayStartPoint.parse("BEGINNING").isBeginning());
        assertTrue(ReplayStartPoint.parse("beginning").isBeginning());
        assertTrue(ReplayStartPoint.parse("  Beginning  ").isBeginning());
    }

    @Test
    @DisplayName("an ISO-8601 instant parses to that instant")
    void parsesInstant() {
        ReplayStartPoint point = ReplayStartPoint.parse("2026-08-23T10:15:30Z");

        assertFalse(point.isBeginning());
        assertEquals(Instant.parse("2026-08-23T10:15:30Z"), point.getFrom());
    }

    @Test
    @DisplayName("nothing configured means no replay, not an error")
    void blankMeansNoReplay() {
        assertNull(ReplayStartPoint.parse(null));
        assertNull(ReplayStartPoint.parse(""));
        assertNull(ReplayStartPoint.parse("   "));
    }

    @Test
    @DisplayName("an unparseable value fails at startup, naming what was expected")
    void badValueFails() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ReplayStartPoint.parse("last tuesday"));

        assertTrue(ex.getMessage().contains("BEGINNING"));
        assertTrue(ex.getMessage().contains("ISO-8601"));
    }

    @Test
    @DisplayName("compares by value, so a container can tell a real change from a repeat")
    void comparesByValue() {
        assertEquals(ReplayStartPoint.beginning(), ReplayStartPoint.parse("BEGINNING"));
        assertEquals(ReplayStartPoint.from(Instant.parse("2026-08-23T10:15:30Z")),
                ReplayStartPoint.parse("2026-08-23T10:15:30Z"));
        assertEquals(ReplayStartPoint.beginning().hashCode(), ReplayStartPoint.beginning().hashCode());
    }

    @Test
    @DisplayName("reads back as what was configured, for log lines")
    void readableToString() {
        assertEquals("BEGINNING", ReplayStartPoint.beginning().toString());
        assertEquals("2026-08-23T10:15:30Z",
                ReplayStartPoint.parse("2026-08-23T10:15:30Z").toString());
    }
}
