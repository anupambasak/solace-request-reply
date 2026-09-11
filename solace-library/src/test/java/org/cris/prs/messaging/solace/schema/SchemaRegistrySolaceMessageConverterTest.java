package org.cris.prs.messaging.solace.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The converter's routing rules, one test per row of the outbound and inbound tables in its javadoc. Uses
 * fake codecs, so no registry and no Apicurio jar is involved.
 */
class SchemaRegistrySolaceMessageConverterTest {

    /** A payload the registry knows nothing about: an ordinary DTO. */
    public static class Order {

        private String id;

        private int quantity;

        public Order() {
        }

        Order(String id, int quantity) {
            this.id = id;
            this.quantity = quantity;
        }

        public String getId() {
            return this.id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getQuantity() {
            return this.quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Order order && Objects.equals(this.id, order.id) && this.quantity == order.quantity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.id, this.quantity);
        }
    }

    /** Stands in for a generated Protobuf message class: a type only one format recognises. */
    public static class FakeProto {

        public String name = "p";
    }

    private final ObjectMapper mapper = new ObjectMapper();

    private FakeSchemaCodec json;

    private FakeSchemaCodec protobuf;

    private SchemaRegistrySolaceMessageConverter converter;

    @BeforeEach
    void setUp() {
        this.json = new FakeSchemaCodec(SchemaFormat.JSON_SCHEMA, JsonNode.class);
        this.protobuf = new FakeSchemaCodec(SchemaFormat.PROTOBUF, FakeProto.class);
        this.converter = new SchemaRegistrySolaceMessageConverter(SchemaCodecs.of(this.json, this.protobuf), this.mapper);
        this.converter.setDestinations(List.of("orders/>"));
    }

    private static byte[] bodyOf(XMLMessage message) {
        return JacksonSolaceMessageConverter.bodyOf((BytesXMLMessage) message);
    }

    private static String formatOf(XMLMessage message) throws Exception {
        return message.getProperties() == null ? null
                : message.getProperties().getString(SchemaRegistryHeaders.SCHEMA_FORMAT);
    }

    @Nested
    @DisplayName("outbound")
    class Outbound {

        @Test
        @DisplayName("null, byte[] and String payloads never reach the registry")
        void passThroughPayloads() {
            converter.toMessage(null, "orders/place");
            converter.toMessage(new byte[] {1, 2}, "orders/place");
            converter.toMessage("text", "orders/place");

            assertTrue(json.serializedTo.isEmpty());
            assertTrue(protobuf.serializedTo.isEmpty());
        }

        @Test
        @DisplayName("a POJO to a governed destination is JSON Schema: framed, and labelled JSON")
        void governedPojo() throws Exception {
            XMLMessage message = converter.toMessage(new Order("o-1", 3), "orders/place");

            assertEquals(List.of("orders/place"), json.serializedTo);
            assertTrue(SchemaRegistryHeaders.isFramed(bodyOf(message)));
            assertEquals("JSON", formatOf(message));
        }

        @Test
        @DisplayName("a queue: prefix is resolved before matching and before the codec sees it")
        void queuePrefixResolved() {
            converter.setDestinations(List.of("orders-q"));

            converter.toMessage(new Order("o-1", 3), "queue:orders-q");

            assertEquals(List.of("orders-q"), json.serializedTo);
        }

        @Test
        @DisplayName("a POJO to an ungoverned destination is plain JSON, unframed and unlabelled")
        void ungovernedPojo() throws Exception {
            XMLMessage message = converter.toMessage(new Order("o-1", 3), "billing/invoice");

            assertTrue(json.serializedTo.isEmpty());
            assertFalse(SchemaRegistryHeaders.isFramed(bodyOf(message)));
            assertEquals(null, formatOf(message));
        }

        @Test
        @DisplayName("a native value goes through its own format whatever its destination")
        void nativeValueChoosesItsFormat() throws Exception {
            XMLMessage proto = converter.toMessage(new FakeProto(), "billing/invoice");
            converter.toMessage(mapper.createObjectNode().put("id", "o-1"), "billing/invoice");

            assertEquals(List.of("billing/invoice"), protobuf.serializedTo);
            assertEquals(List.of("billing/invoice"), json.serializedTo);
            assertEquals("PROTOBUF", formatOf(proto));
        }

        @Test
        @DisplayName("no destinations configured means every destination is governed")
        void emptyDestinationsGovernEverything() {
            converter.setDestinations(List.of());

            converter.toMessage(new Order("o-1", 3), "anything/at/all");

            assertEquals(1, json.serializedTo.size());
        }

        @Test
        @DisplayName("a POJO to a governed destination without JSON Schema enabled is a type mismatch")
        void pojoWithoutJsonSchema() {
            SchemaRegistrySolaceMessageConverter protobufOnly =
                    new SchemaRegistrySolaceMessageConverter(SchemaCodecs.of(protobuf), mapper);

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> protobufOnly.toMessage(new Order("o-1", 3), "orders/place"));

            assertEquals(SchemaRegistryConversionException.Reason.TYPE_MISMATCH, failure.getReason());
        }

        @Test
        @DisplayName("a POJO to a topic whose POJO format is AVRO goes to an Avro codec that accepts POJOs")
        void pojoFormatPerTopic() throws Exception {
            FakeSchemaCodec avro = new FakeSchemaCodec(SchemaFormat.AVRO, FakeProto.class);
            avro.acceptsPojos = true;
            SchemaRegistrySolaceMessageConverter withAvro =
                    new SchemaRegistrySolaceMessageConverter(SchemaCodecs.of(json, avro), mapper);
            withAvro.setDestinations(List.of("orders/>"));
            withAvro.setPojoFormats(java.util.Map.of("orders/avro/>", SchemaFormat.AVRO));

            XMLMessage message = withAvro.toMessage(new Order("o-1", 3), "orders/avro/place");
            withAvro.toMessage(new Order("o-2", 1), "orders/place");

            assertEquals(List.of("orders/avro/place"), avro.serializedTo);
            assertEquals(List.of("orders/place"), json.serializedTo);
            assertEquals("AVRO", formatOf(message));
        }

        @Test
        @DisplayName("a POJO format whose codec cannot write POJOs is a type mismatch")
        void pojoFormatWithoutPojoSupport() {
            FakeSchemaCodec avro = new FakeSchemaCodec(SchemaFormat.AVRO, FakeProto.class);
            SchemaRegistrySolaceMessageConverter withAvro =
                    new SchemaRegistrySolaceMessageConverter(SchemaCodecs.of(json, avro), mapper);
            withAvro.setPojoFormats(java.util.Map.of("orders/>", SchemaFormat.AVRO));

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> withAvro.toMessage(new Order("o-1", 3), "orders/place"));

            assertEquals(SchemaRegistryConversionException.Reason.TYPE_MISMATCH, failure.getReason());
        }

        @Test
        @DisplayName("a registry that cannot be reached is classified as retryable")
        void registryDownIsRetryable() {
            json.failWith = new IllegalStateException("lookup failed", new ConnectException("refused"));

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> converter.toMessage(new Order("o-1", 3), "orders/place"));

            assertEquals(SchemaRegistryConversionException.Reason.REGISTRY_UNAVAILABLE, failure.getReason());
            assertTrue(failure.getReason().isRetryable());
        }

        @Test
        @DisplayName("a reason raised inside the serde, by the topic strategy, survives classification")
        void innerReasonSurvives() {
            json.failWith = new RuntimeException("resolver failed", new SchemaRegistryConversionException(
                    SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND, "no mapping", null));

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> converter.toMessage(new Order("o-1", 3), "orders/place"));

            assertEquals(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND, failure.getReason());
        }
    }

    @Nested
    @DisplayName("inbound")
    class Inbound {

        private final byte[] plain = "{\"id\":\"o-1\",\"quantity\":3}".getBytes(StandardCharsets.UTF_8);

        private final byte[] framed = FakeSchemaCodec.frame(this.plain);

        @Test
        @DisplayName("a framed JSON message is decoded through the registry onto the listener's type")
        void framedJson() {
            Object order = converter.fromMessage(FakeSchemaCodec.received("orders/place", framed, "JSON"), Order.class);

            assertEquals(new Order("o-1", 3), order);
            assertEquals(1, json.deserializeCalls);
        }

        @Test
        @DisplayName("the schemaFormat property picks the codec")
        void propertyPicksTheCodec() {
            converter.fromMessage(FakeSchemaCodec.received("orders/place", framed, "PROTOBUF"), JsonNode.class);

            assertEquals(1, protobuf.deserializeCalls);
            assertEquals(0, json.deserializeCalls);
        }

        @Test
        @DisplayName("without the property, a format-specific listener type picks the codec")
        void targetTypePicksTheCodec() {
            byte[] proto = FakeSchemaCodec.frame("{\"name\":\"x\"}".getBytes(StandardCharsets.UTF_8));

            Object value = converter.fromMessage(FakeSchemaCodec.received("orders/place", proto, null), FakeProto.class);

            assertInstanceOf(FakeProto.class, value);
            assertEquals(1, protobuf.deserializeCalls);
            assertEquals(0, json.deserializeCalls);
        }

        @Test
        @DisplayName("without the property or a specific type, JSON Schema is the default")
        void jsonIsTheDefault() {
            Object order = converter.fromMessage(FakeSchemaCodec.received("orders/place", framed, null), Order.class);

            assertEquals(new Order("o-1", 3), order);
            assertEquals(1, json.deserializeCalls);
        }

        @Test
        @DisplayName("a declared format that is not enabled is rejected, not guessed")
        void unsupportedFormat() {
            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> converter.fromMessage(FakeSchemaCodec.received("orders/place", framed, "AVRO"), Order.class));

            assertEquals(SchemaRegistryConversionException.Reason.UNSUPPORTED_FORMAT, failure.getReason());
            assertFalse(failure.getReason().isRetryable());
        }

        @Test
        @DisplayName("an unframed message falls back to plain JSON")
        void unframed() {
            Object order = converter.fromMessage(FakeSchemaCodec.received("orders/place", plain, null), Order.class);

            assertEquals(new Order("o-1", 3), order);
            assertEquals(0, json.deserializeCalls);
        }

        @Test
        @DisplayName("strict mode rejects an unframed message on a governed destination")
        void strictRejectsUnframed() {
            converter.setRequireSchemaId(true);

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> converter.fromMessage(FakeSchemaCodec.received("orders/place", plain, null), Order.class));

            assertEquals(SchemaRegistryConversionException.Reason.MISSING_SCHEMA_ID, failure.getReason());
        }

        @Test
        @DisplayName("strict mode leaves an ungoverned destination alone")
        void strictIgnoresUngoverned() {
            converter.setRequireSchemaId(true);

            Object order = converter.fromMessage(FakeSchemaCodec.received("billing/invoice", plain, null), Order.class);

            assertEquals(new Order("o-1", 3), order);
        }

        @Test
        @DisplayName("String, byte[] and raw-message targets never reach the registry")
        void rawTargets() {
            BytesXMLMessage message = FakeSchemaCodec.received("orders/place", framed, "JSON");

            assertArrayEquals(framed, (byte[]) converter.fromMessage(message, byte[].class));
            assertSame(message, converter.fromMessage(message, BytesXMLMessage.class));
            assertInstanceOf(String.class, converter.fromMessage(message, String.class));
            assertEquals(0, json.deserializeCalls);
        }

        @Test
        @DisplayName("valid JSON that does not fit the listener's type is a non-retryable type mismatch")
        void unmappableIsTypeMismatch() {
            byte[] wrong = FakeSchemaCodec.frame("{\"quantity\":\"many\"}".getBytes(StandardCharsets.UTF_8));

            SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                    () -> converter.fromMessage(FakeSchemaCodec.received("orders/place", wrong, "JSON"), Order.class));

            assertEquals(SchemaRegistryConversionException.Reason.TYPE_MISMATCH, failure.getReason());
        }
    }
}
