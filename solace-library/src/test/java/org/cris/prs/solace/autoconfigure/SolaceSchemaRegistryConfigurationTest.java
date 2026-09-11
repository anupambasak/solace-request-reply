package org.cris.prs.solace.autoconfigure;

import org.cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceMessageConverter;
import org.cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.cris.prs.messaging.solace.schema.AvroSchemaCodec;
import org.cris.prs.messaging.solace.schema.JsonSchemaCodec;
import org.cris.prs.messaging.solace.schema.ProtobufSchemaCodec;
import org.cris.prs.messaging.solace.schema.SchemaCodec;
import org.cris.prs.messaging.solace.schema.SchemaCodecs;
import org.cris.prs.messaging.solace.schema.SchemaFormat;
import org.cris.prs.messaging.solace.schema.SchemaRegistryErrorHandler;
import org.cris.prs.messaging.solace.schema.SchemaRegistrySolaceMessageConverter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The conditions of {@link SolaceSchemaRegistryConfiguration}, and the import order that lets its converter
 * replace the core one. None of this contacts a registry: Apicurio serdes are created lazily.
 */
class SolaceSchemaRegistryConfigurationTest {

    private static final String[] ENABLED = {
            "solace.schema-registry.url=http://localhost:1/apis/registry/v3",
            "solace.schema-registry.destinations[0]=orders/>",
            "solace.schema-registry.topic-profile[0].topic-expression=orders/>",
            "solace.schema-registry.topic-profile[0].artifact-id=order",
            "solace.request-reply.enabled=false"};

    /** What SolaceAutoConfiguration looks like to this configuration: it imports it, then backs off. */
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(SolaceProperties.class)
    @Import(SolaceSchemaRegistryConfiguration.class)
    static class CoreLikeConfiguration {

        @Bean
        @ConditionalOnMissingBean
        SolaceMessageConverter solaceMessageConverter() {
            return new JacksonSolaceMessageConverter();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FakeCodecConfiguration {

        @Bean
        SchemaCodecs solaceSchemaCodecs() {
            return SchemaCodecs.of(new SchemaCodec() {
                @Override
                public SchemaFormat getFormat() {
                    return SchemaFormat.JSON_SCHEMA;
                }

                @Override
                public boolean isSchemaPayload(Object payload) {
                    return false;
                }

                @Override
                public boolean producesType(Class<?> targetType) {
                    return false;
                }

                @Override
                public byte[] serialize(String destinationName, Object payload) {
                    return new byte[0];
                }

                @Override
                public Object deserialize(String destinationName, byte[] body, Class<?> targetType) {
                    return null;
                }
            });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UserConverterConfiguration {

        static final SolaceMessageConverter USER = new JacksonSolaceMessageConverter();

        @Bean
        SolaceMessageConverter myConverter() {
            return USER;
        }
    }

    /**
     * Application configurations first, then the core: in a real application the auto-configuration is
     * deferred until every user configuration is registered, which is what lets a user bean win.
     */
    private static ApplicationContextRunner runner(Class<?>... applicationConfigurations) {
        return new ApplicationContextRunner()
                .withUserConfiguration(applicationConfigurations)
                .withUserConfiguration(CoreLikeConfiguration.class);
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }

    @Test
    @DisplayName("without solace.schema-registry.url nothing changes")
    void disabledWithoutUrl() {
        runner().run(context -> {
            assertFalse(context.containsBean("solaceSchemaCodecs"));
            assertInstanceOf(JacksonSolaceMessageConverter.class, context.getBean(SolaceMessageConverter.class));
        });
    }

    @Test
    @DisplayName("with a URL the registry converter replaces the core one, and rejects poison messages")
    void enabled() {
        runner(FakeCodecConfiguration.class).withPropertyValues(ENABLED).run(context -> {
            SchemaRegistrySolaceMessageConverter registry = assertInstanceOf(
                    SchemaRegistrySolaceMessageConverter.class, context.getBean(SolaceMessageConverter.class));
            assertSame(context.getBean(SchemaCodecs.class), registry.getCodecs());
            assertTrue(registry.isGoverned("orders/place"));
            assertFalse(registry.isGoverned("billing/invoice"));
            assertInstanceOf(SchemaRegistryErrorHandler.class, context.getBean(SolaceListenerErrorHandler.class));
        });
    }

    @Test
    @DisplayName("a converter the application declares wins")
    void userConverterWins() {
        runner(FakeCodecConfiguration.class, UserConverterConfiguration.class).withPropertyValues(ENABLED)
                .run(context -> assertSame(UserConverterConfiguration.USER, context.getBean(SolaceMessageConverter.class)));
    }

    @Test
    @DisplayName("enables every format whose Apicurio module is present, without contacting the registry")
    void realCodecsForEveryFormat() {
        runner().withPropertyValues(ENABLED).run(context -> {
            assertNull(context.getStartupFailure(), () -> "startup failed: " + context.getStartupFailure());
            SchemaCodecs codecs = context.getBean(SchemaCodecs.class);
            assertInstanceOf(AvroSchemaCodec.class, codecs.get(SchemaFormat.AVRO));
            assertInstanceOf(ProtobufSchemaCodec.class, codecs.get(SchemaFormat.PROTOBUF));
            assertInstanceOf(JsonSchemaCodec.class, codecs.get(SchemaFormat.JSON_SCHEMA));
        });
    }

    @Test
    @DisplayName("formats restricts the enabled codecs")
    void formatsRestrict() {
        runner().withPropertyValues(ENABLED).withPropertyValues("solace.schema-registry.formats=PROTOBUF")
                .run(context -> {
                    SchemaCodecs codecs = context.getBean(SchemaCodecs.class);
                    assertEquals(1, codecs.all().size());
                    assertNotNull(codecs.get(SchemaFormat.PROTOBUF));
                });
    }

    @Test
    @DisplayName("an incomplete configuration fails startup, naming the property")
    void invalidConfigurationFailsStartup() {
        runner().withPropertyValues("solace.schema-registry.url=http://localhost:1/apis/registry/v3").run(context -> {
            assertNotNull(context.getStartupFailure());
            String message = rootCause(context.getStartupFailure()).getMessage();
            assertTrue(message.contains("solace.schema-registry.topic-profile"), message);
        });
    }

    @Test
    @DisplayName("SolaceAutoConfiguration imports the registry configuration before anything else")
    void importedFirst() {
        Import imports = SolaceAutoConfiguration.class.getAnnotation(Import.class);

        assertEquals(SolaceSchemaRegistryConfiguration.class, imports.value()[0]);
    }
}
