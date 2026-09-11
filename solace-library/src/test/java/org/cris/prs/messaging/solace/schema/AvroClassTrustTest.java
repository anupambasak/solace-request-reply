package org.cris.prs.messaging.solace.schema;

import org.apache.avro.util.ClassSecurityValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Avro 1.11.4+ refuses to instantiate classes it does not trust, which broke reflection over shared DTOs
 * with "Forbidden cris.prs.messaging.Person! This class is not trusted to be included in Avro schemas".
 */
class AvroClassTrustTest {

    static class SentPayload {
    }

    static class NeverSeen {
    }

    @Test
    @DisplayName("a class the application uses becomes trusted; others stay forbidden")
    void trustsWhatTheApplicationUses() {
        assertThrows(SecurityException.class, () -> ClassSecurityValidator.validate(NeverSeen.class));

        AvroClassTrust.trust(SentPayload.class);

        assertDoesNotThrow(() -> ClassSecurityValidator.validate(SentPayload.class));
        assertThrows(SecurityException.class, () -> ClassSecurityValidator.validate(NeverSeen.class));
    }

    @Test
    @DisplayName("a trusted package covers its classes and sub-packages, not look-alike prefixes")
    void trustsPackages() {
        AvroClassTrust.trustPackages(List.of("java.util.concurrent"));

        assertDoesNotThrow(() -> ClassSecurityValidator.validate(java.util.concurrent.ConcurrentHashMap.class));
        assertDoesNotThrow(() -> ClassSecurityValidator.validate(java.util.concurrent.atomic.AtomicLong.class));
        assertThrows(SecurityException.class, () -> ClassSecurityValidator.validate(java.util.HashMap.class));
    }
}
