package org.cris.prs.messaging.solace.schema;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When declared schemas reach the registry, and what a failure does. Nothing here contacts a registry: a
 * schema whose location cannot be read fails before any client is created, which is the point &mdash; a
 * typo in a location should not need a running registry to be reported.
 */
class SchemaArtifactRegistrarTest {

    private static SchemaRegistrySettings withSchema(String location,
            SchemaRegistrySettings.RegistrationMode mode, boolean failFast) {
        SchemaRegistrySettings settings = SchemaRegistrySettingsTest.valid();
        SchemaRegistrySettings.DeclaredSchema schema = new SchemaRegistrySettings.DeclaredSchema();
        schema.setArtifactId("order");
        schema.setGroupId("orders");
        schema.setFormat(SchemaFormat.JSON_SCHEMA);
        schema.setLocation(location);
        settings.getRegistration().getSchemas().add(schema);
        settings.getRegistration().setMode(mode);
        settings.getRegistration().setFailFast(failFast);
        return settings;
    }

    @Test
    @DisplayName("declaring nothing is the default, and every call is a no-op")
    void nothingDeclared() {
        SchemaRegistrySettings settings = SchemaRegistrySettingsTest.valid();
        SchemaArtifactRegistrar registrar = new SchemaArtifactRegistrar(settings);

        assertEquals(SchemaRegistrySettings.RegistrationMode.FIRST_MESSAGE,
                settings.getRegistration().getMode());
        assertFalse(registrar.hasSchemas());
        assertTrue(registrar.isRegistered());
        assertDoesNotThrow(registrar::afterPropertiesSet);
        assertDoesNotThrow(registrar::registerOnce);
        assertDoesNotThrow(registrar::register);
    }

    @Test
    @DisplayName("FIRST_MESSAGE does nothing at startup, however broken the declaration")
    void firstMessageDoesNothingAtStartup() {
        SchemaArtifactRegistrar registrar = new SchemaArtifactRegistrar(
                withSchema("classpath:no/such/schema.json",
                        SchemaRegistrySettings.RegistrationMode.FIRST_MESSAGE, true));

        assertDoesNotThrow(registrar::afterPropertiesSet);
        assertFalse(registrar.isRegistered());
    }

    @Test
    @DisplayName("STARTUP publishes as the bean initialises, and fail-fast lets the failure stop the context")
    void startupFailFast() {
        SchemaArtifactRegistrar registrar = new SchemaArtifactRegistrar(
                withSchema("classpath:no/such/schema.json",
                        SchemaRegistrySettings.RegistrationMode.STARTUP, true));

        SchemaRegistryConversionException ex = assertThrows(SchemaRegistryConversionException.class,
                registrar::afterPropertiesSet);

        assertEquals(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND, ex.getReason());
    }

    @Test
    @DisplayName("without fail-fast a failed attempt is a warning, and the next call tries again")
    void failureIsRetried() {
        SchemaArtifactRegistrar registrar = new SchemaArtifactRegistrar(
                withSchema("classpath:no/such/schema.json",
                        SchemaRegistrySettings.RegistrationMode.STARTUP, false));

        assertDoesNotThrow(registrar::afterPropertiesSet);
        assertFalse(registrar.isRegistered(), "a failed attempt must not count as registered");
        assertDoesNotThrow(registrar::registerOnce);
    }

    @Test
    @DisplayName("an incomplete declaration fails startup validation, naming what is missing")
    void validation() {
        SchemaRegistrySettings settings = SchemaRegistrySettingsTest.valid();
        SchemaRegistrySettings.DeclaredSchema schema = new SchemaRegistrySettings.DeclaredSchema();
        schema.setArtifactId("order");
        settings.getRegistration().getSchemas().add(schema);

        IllegalStateException ex = assertThrows(IllegalStateException.class, settings::validate);

        assertTrue(ex.getMessage().contains("registration.schemas"), ex.getMessage());
    }
}
