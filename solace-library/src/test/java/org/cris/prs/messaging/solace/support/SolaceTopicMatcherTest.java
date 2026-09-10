package org.cris.prs.messaging.solace.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Solace wildcard semantics, which topic dispatch depends on getting exactly right: routing a message
 * to the wrong handler is worse than not routing it at all.
 */
class SolaceTopicMatcherTest {

    @Test
    @DisplayName("an exact topic matches itself and nothing else")
    void exactMatch() {
        assertTrue(SolaceTopicMatcher.matches("orders/created", "orders/created"));
        assertFalse(SolaceTopicMatcher.matches("orders/created", "orders/cancelled"));
        assertFalse(SolaceTopicMatcher.matches("orders/created", "orders/created/eu"));
        assertFalse(SolaceTopicMatcher.matches("orders/created", "orders"));
    }

    @Nested
    @DisplayName("* matches exactly one level")
    class SingleLevel {

        @Test
        @DisplayName("one level, no more and no fewer")
        void exactlyOneLevel() {
            assertTrue(SolaceTopicMatcher.matches("orders/*", "orders/created"));
            assertFalse(SolaceTopicMatcher.matches("orders/*", "orders/eu/created"));
            assertFalse(SolaceTopicMatcher.matches("orders/*", "orders"));
        }

        @Test
        @DisplayName("matches in the middle as well as at the end")
        void inTheMiddle() {
            assertTrue(SolaceTopicMatcher.matches("orders/*/created", "orders/eu/created"));
            assertFalse(SolaceTopicMatcher.matches("orders/*/created", "orders/eu/fr/created"));
        }

        @Test
        @DisplayName("a prefix before * matches within the level only")
        void prefixWithinLevel() {
            assertTrue(SolaceTopicMatcher.matches("orders/cr*", "orders/created"));
            assertTrue(SolaceTopicMatcher.matches("orders/cr*", "orders/cr"));
            assertFalse(SolaceTopicMatcher.matches("orders/cr*", "orders/cancelled"));
            assertFalse(SolaceTopicMatcher.matches("orders/cr*", "orders/cr/eu"));
        }
    }

    @Nested
    @DisplayName("> matches one or more trailing levels")
    class MultiLevel {

        @Test
        @DisplayName("one level or many, but never zero")
        void oneOrMore() {
            assertTrue(SolaceTopicMatcher.matches("orders/>", "orders/created"));
            assertTrue(SolaceTopicMatcher.matches("orders/>", "orders/eu/created"));
            assertTrue(SolaceTopicMatcher.matches("orders/>", "orders/eu/fr/created/v2"));
            assertFalse(SolaceTopicMatcher.matches("orders/>", "orders"),
                    "> needs at least one level to consume");
        }

        @Test
        @DisplayName("a bare > matches everything")
        void bareWildcard() {
            assertTrue(SolaceTopicMatcher.matches(">", "orders"));
            assertTrue(SolaceTopicMatcher.matches(">", "orders/eu/created"));
        }

        @Test
        @DisplayName("does not match a different prefix")
        void differentPrefix() {
            assertFalse(SolaceTopicMatcher.matches("orders/>", "payments/created"));
        }
    }

    @Test
    @DisplayName("the two wildcards combine")
    void combined() {
        assertTrue(SolaceTopicMatcher.matches("orders/*/audit/>", "orders/eu/audit/created"));
        assertTrue(SolaceTopicMatcher.matches("orders/*/audit/>", "orders/eu/audit/created/v2"));
        assertFalse(SolaceTopicMatcher.matches("orders/*/audit/>", "orders/eu/fr/audit/created"));
        assertFalse(SolaceTopicMatcher.matches("orders/*/audit/>", "orders/eu/audit"));
    }

    @Test
    @DisplayName("null is never a match, rather than an exception on the message path")
    void nullsAreNotMatches() {
        assertFalse(SolaceTopicMatcher.matches(null, "orders/created"));
        assertFalse(SolaceTopicMatcher.matches("orders/>", null));
    }
}
