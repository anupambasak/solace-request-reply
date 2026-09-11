package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.data.SerdeMetadata;
import io.apicurio.registry.serde.data.SerdeRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Topic-expression artifact resolution, which is what makes per-instance reply topics work. */
class SolaceTopicProfileStrategyTest {

    private static SchemaRegistrySettings.TopicMapping mapping(String expression, String group, String artifact) {
        SchemaRegistrySettings.TopicMapping mapping = new SchemaRegistrySettings.TopicMapping();
        mapping.setTopicExpression(expression);
        mapping.setGroupId(group);
        mapping.setArtifactId(artifact);
        return mapping;
    }

    private final SolaceTopicProfileStrategy<Object> strategy = new SolaceTopicProfileStrategy<>(List.of(
            mapping("orders/place", null, "order-request"),
            mapping("app/reply/>", "replies", "order-reply"),
            mapping("orders/>", null, "order-any")));

    private ArtifactReference resolve(String topic) {
        return this.strategy.artifactReference(new SerdeRecord<>(new SerdeMetadata(topic, false), new Object()), null);
    }

    @Test
    @DisplayName("every instance's reply topic resolves to the one artifact mapped by the prefix")
    void perInstanceReplyTopics() {
        assertEquals("order-reply", resolve("app/reply/pod-1").getArtifactId());
        assertEquals("order-reply", resolve("app/reply/pod-2").getArtifactId());
        assertEquals("replies", resolve("app/reply/pod-2").getGroupId());
    }

    @Test
    @DisplayName("first match wins, in declaration order")
    void firstMatchWins() {
        assertEquals("order-request", resolve("orders/place").getArtifactId());
        assertEquals("order-any", resolve("orders/cancel").getArtifactId());
        assertEquals("default", resolve("orders/place").getGroupId());   // Apicurio's builder fills it in
    }

    @Test
    @DisplayName("an unmapped topic is a non-retryable SCHEMA_NOT_FOUND")
    void unmapped() {
        SchemaRegistryConversionException failure = assertThrows(SchemaRegistryConversionException.class,
                () -> resolve("billing/invoice"));

        assertEquals(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND, failure.getReason());
    }
}
