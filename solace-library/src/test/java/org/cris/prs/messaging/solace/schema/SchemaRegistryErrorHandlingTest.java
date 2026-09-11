package org.cris.prs.messaging.solace.schema;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.cris.prs.messaging.solace.core.SettlementOutcome;
import org.cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Classification of serde failures, and the outcome the error handler derives from it. */
class SchemaRegistryErrorHandlingTest {

    /** Stands in for a validation exception, which is matched by name. */
    static class JsonSchemaValidationException extends RuntimeException {
        JsonSchemaValidationException(String message) {
            super(message);
        }
    }

    /** Stands in for Kiota's ApiException, which Apicurio's registry client throws: read reflectively. */
    public static class FakeApiException extends RuntimeException {

        private final int status;

        FakeApiException(int status) {
            super("HTTP " + status);
            this.status = status;
        }

        public int getResponseStatusCode() {
            return this.status;
        }
    }

    private final BytesXMLMessage message =
            FakeSchemaCodec.received("orders/place", "{}".getBytes(StandardCharsets.UTF_8), "JSON");

    @Test
    @DisplayName("classifies by what the cause chain contains")
    void classifies() {
        assertEquals(SchemaRegistryConversionException.Reason.VALIDATION_FAILED,
                SchemaRegistryConversionException.classify("x",
                        new RuntimeException(new JsonSchemaValidationException("required property 'id'"))).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND,
                SchemaRegistryConversionException.classify("x",
                        new IllegalStateException("Artifact not found: order")).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.REGISTRY_UNAVAILABLE,
                SchemaRegistryConversionException.classify("x",
                        new RuntimeException(new SocketTimeoutException("read timed out"))).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.UNCLASSIFIED,
                SchemaRegistryConversionException.classify("x", new IllegalStateException("?")).getReason());
    }

    @Test
    @DisplayName("classifies a registry HTTP status, found anywhere in the chain")
    void classifiesHttpStatus() {
        assertEquals(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND,
                SchemaRegistryConversionException.classify("x", new RuntimeException(new FakeApiException(404))).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.REGISTRY_UNAVAILABLE,
                SchemaRegistryConversionException.classify("x", new FakeApiException(503)).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.REGISTRY_UNAVAILABLE,
                SchemaRegistryConversionException.classify("x", new FakeApiException(401)).getReason());
        assertEquals(SchemaRegistryConversionException.Reason.VALIDATION_FAILED,
                SchemaRegistryConversionException.classify("x", new java.io.UncheckedIOException(new java.io.IOException(
                        "Error validating data against json schema with message: $.id: is missing"))).getReason());
    }

    @Test
    @DisplayName("rejects what a retry cannot fix, even when the listener wrapped it")
    void rejectsPoison() {
        SchemaRegistryErrorHandler handler = new SchemaRegistryErrorHandler();
        Exception wrapped = new RuntimeException("listener failed", new SchemaRegistryConversionException(
                SchemaRegistryConversionException.Reason.VALIDATION_FAILED, "bad", null));

        assertEquals(SettlementOutcome.REJECTED, handler.resolveOutcome(this.message, wrapped));
    }

    @Test
    @DisplayName("defers a retryable schema failure, and anything else, to the delegate")
    void defersTheRest() {
        SolaceListenerErrorHandler delegate = new SolaceListenerErrorHandler() {
            @Override
            public void handleError(BytesXMLMessage message, Exception exception) {
            }

            @Override
            public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
                return SettlementOutcome.FAILED;
            }
        };
        SchemaRegistryErrorHandler handler = new SchemaRegistryErrorHandler(delegate);

        assertEquals(SettlementOutcome.FAILED, handler.resolveOutcome(this.message,
                new SchemaRegistryConversionException(
                        SchemaRegistryConversionException.Reason.REGISTRY_UNAVAILABLE, "down", null)));
        assertEquals(SettlementOutcome.FAILED,
                handler.resolveOutcome(this.message, new IllegalStateException("downstream")));
        assertNull(new SchemaRegistryErrorHandler().resolveOutcome(this.message, new IllegalStateException()));
    }
}
