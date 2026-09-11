package org.cris.prs.messaging.solace.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Startup validation: every configuration that cannot work fails naming the property to change. */
class SchemaRegistrySettingsTest {

    static SchemaRegistrySettings valid() {
        SchemaRegistrySettings settings = new SchemaRegistrySettings();
        settings.setUrl("http://registry:8080/apis/registry/v3");
        SchemaRegistrySettings.TopicMapping mapping = new SchemaRegistrySettings.TopicMapping();
        mapping.setTopicExpression("orders/>");
        mapping.setArtifactId("order");
        settings.getTopicProfile().add(mapping);
        return settings;
    }

    @Test
    @DisplayName("a complete configuration validates, with the two library defaults")
    void validConfiguration() {
        SchemaRegistrySettings settings = valid();

        assertDoesNotThrow(settings::validate);
        assertEquals(Boolean.TRUE, settings.getCache().getFaultTolerantRefresh());
        assertEquals(SchemaRegistrySettings.HttpAdapter.JDK, settings.getHttpAdapter());
        assertEquals(SchemaRegistrySettings.STRATEGY_TOPIC_PROFILE, settings.getArtifactResolverStrategy());
    }

    @Test
    @DisplayName("the URL is required")
    void urlRequired() {
        SchemaRegistrySettings settings = valid();
        settings.setUrl(" ");

        assertTrue(assertThrows(IllegalStateException.class, settings::validate).getMessage().contains("url"));
    }

    @Test
    @DisplayName("TOPIC_PROFILE with no mappings and no explicit artifact resolves nothing")
    void emptyTopicProfile() {
        SchemaRegistrySettings settings = valid();
        settings.getTopicProfile().clear();

        assertTrue(assertThrows(IllegalStateException.class, settings::validate).getMessage().contains("topic-profile"));

        settings.getExplicitArtifact().setArtifactId("order");
        assertDoesNotThrow(settings::validate);
    }

    @Test
    @DisplayName("a mapping needs an artifact id")
    void mappingNeedsArtifactId() {
        SchemaRegistrySettings settings = valid();
        settings.getTopicProfile().get(0).setArtifactId(null);

        assertThrows(IllegalStateException.class, settings::validate);
    }

    @Test
    @DisplayName("RECORD is Avro only")
    void recordIsAvroOnly() {
        SchemaRegistrySettings settings = valid();
        settings.setArtifactResolverStrategy("RECORD");

        assertThrows(IllegalStateException.class, settings::validate);

        settings.setFormats(List.of(SchemaFormat.AVRO, SchemaFormat.PROTOBUF));
        assertThrows(IllegalStateException.class, settings::validate);

        settings.setFormats(List.of(SchemaFormat.AVRO));
        assertDoesNotThrow(settings::validate);
    }

    @Test
    @DisplayName("artifact types map back to formats")
    void artifactTypes() {
        assertEquals(SchemaFormat.JSON_SCHEMA, SchemaFormat.fromArtifactType("JSON"));
        assertEquals(SchemaFormat.PROTOBUF, SchemaFormat.fromArtifactType("protobuf"));
        assertEquals(SchemaFormat.AVRO, SchemaFormat.fromArtifactType("AVRO"));
        assertEquals(null, SchemaFormat.fromArtifactType("XML"));
    }
}
