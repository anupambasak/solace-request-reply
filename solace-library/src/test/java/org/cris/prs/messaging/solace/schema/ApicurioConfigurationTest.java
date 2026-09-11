package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Translation of the bound settings into Apicurio's configuration keys. */
class ApicurioConfigurationTest {

    @Test
    @DisplayName("an untouched configuration writes only the URL, the strategy and the two library defaults")
    void untouchedIsMinimal() {
        Map<String, Object> config = ApicurioConfiguration.common(SchemaRegistrySettingsTest.valid());

        assertEquals("http://registry:8080/apis/registry/v3", config.get(SchemaResolverConfig.REGISTRY_URL));
        assertInstanceOf(SolaceTopicProfileStrategy.class, config.get(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY));
        assertEquals(Boolean.TRUE, config.get(SchemaResolverConfig.FAULT_TOLERANT_REFRESH));
        assertEquals("JDK", config.get(SchemaResolverConfig.HTTP_ADAPTER));
        assertEquals(4, config.size());
    }

    @Test
    @DisplayName("durations become millis, enums become Apicurio's spelling, raw properties win")
    void translatesAndOverrides() {
        SchemaRegistrySettings settings = SchemaRegistrySettingsTest.valid();
        settings.getCache().setCheckPeriod(Duration.ofSeconds(5));
        settings.getRetry().setBackoff(Duration.ofMillis(250));
        settings.setUseId(SchemaRegistrySettings.IdOption.GLOBAL_ID);
        settings.setAutoRegister(Boolean.TRUE);
        settings.getProperties().put(SchemaResolverConfig.RETRY_COUNT, "7");

        Map<String, Object> config = ApicurioConfiguration.common(settings);

        assertEquals(5000L, config.get(SchemaResolverConfig.CHECK_PERIOD_MS));
        assertEquals(250L, config.get(SchemaResolverConfig.RETRY_BACKOFF_MS));
        assertEquals("globalId", config.get(SerdeConfig.USE_ID));
        assertEquals(Boolean.TRUE, config.get(SchemaResolverConfig.AUTO_REGISTER_ARTIFACT));
        assertEquals("7", config.get(SchemaResolverConfig.RETRY_COUNT));
    }

    @Test
    @DisplayName("DESTINATION names Apicurio's SimpleTopicIdStrategy; blank strings are not written")
    void destinationStrategy() {
        SchemaRegistrySettings settings = SchemaRegistrySettingsTest.valid();
        settings.setArtifactResolverStrategy("DESTINATION");
        settings.setUsername("");

        Map<String, Object> config = ApicurioConfiguration.common(settings);

        assertEquals("io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy",
                config.get(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY));
        assertFalse(config.containsKey(SchemaResolverConfig.AUTH_USERNAME));
    }
}
