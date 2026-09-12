package org.cris.prs.solace.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.core.SolaceMessageConverter;
import org.cris.prs.messaging.solace.listener.SolaceListenerErrorHandler;
import org.cris.prs.messaging.solace.schema.SchemaArtifactRegistrar;
import org.cris.prs.messaging.solace.schema.SchemaCodecs;
import org.cris.prs.messaging.solace.schema.SchemaFormat;
import org.cris.prs.messaging.solace.schema.SchemaRegistryErrorHandler;
import org.cris.prs.messaging.solace.schema.SchemaRegistrySettings;
import org.cris.prs.messaging.solace.schema.SchemaRegistrySolaceMessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Apicurio Registry wiring, active when {@code solace.schema-registry.url} is set.
 *
 * <p>Imported by {@link SolaceAutoConfiguration} <em>before</em> its own bean methods, so the
 * registry-aware converter registered here is what the core {@code solaceMessageConverter} sees when its
 * {@code @ConditionalOnMissingBean} is evaluated. A converter the application declares itself still wins
 * over both.</p>
 *
 * <p>There is deliberately no {@code @ConditionalOnClass} on the Apicurio jars. An application that sets a
 * registry URL expects its messages governed; silently falling back to plain JSON because a jar is missing
 * would be the worst outcome, so the codecs bean fails startup instead, naming the missing module. Nothing
 * here contacts the registry at startup.</p>
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "solace.schema-registry", name = "url")
public class SolaceSchemaRegistryConfiguration {

    /** Create the configuration. Instantiated by Spring, not by application code. */
    public SolaceSchemaRegistryConfiguration() {
    }

    /**
     * A codec for every enabled format: Avro, Protobuf, JSON Schema.
     *
     * <p>Closed by Spring on shutdown, which closes every Apicurio serde and its registry client.</p>
     *
     * @param properties   supplies {@code solace.schema-registry.*} and the request-reply settings
     * @param objectMapper the application's mapper when one exists, used by JSON Schema
     * @return the codecs
     * @throws IllegalStateException if the settings are invalid or a format's Apicurio module is missing
     */
    @Bean
    @ConditionalOnMissingBean
    public SchemaCodecs solaceSchemaCodecs(SolaceProperties properties, ObjectProvider<ObjectMapper> objectMapper) {
        SchemaRegistrySettings settings = properties.getSchemaRegistry();
        SchemaCodecs codecs = SchemaCodecs.create(settings, objectMapper.getIfAvailable(ObjectMapper::new),
                SolaceSchemaRegistryConfiguration.class.getClassLoader());
        String strategy = settings.getArtifactResolverStrategy();
        if ((SchemaRegistrySettings.STRATEGY_DESTINATION.equalsIgnoreCase(strategy)
                || SchemaRegistrySettings.STRATEGY_TOPIC.equalsIgnoreCase(strategy))
                && properties.getRequestReply().isEnabled()) {
            log.warn("solace.schema-registry.artifact-resolver-strategy is {} while request-reply is enabled. Reply "
                    + "topics carry the instance id, so every instance's replies resolve to a different artifact. "
                    + "Use TOPIC_PROFILE and map the reply prefix with '>'", strategy);
        }
        log.info("Apicurio Registry enabled: registry {}, strategy {}", settings.getUrl(), strategy);
        return codecs;
    }

    /**
     * Publishes the schemas declared under {@code solace.schema-registry.registration.schemas}.
     *
     * <p>The bean exists whether or not anything is declared, so the converter can always ask it; with no
     * schemas every call is a no-op. When {@code registration.mode} is {@code STARTUP} it publishes them
     * as it is initialised &mdash; the one place in this configuration that contacts the registry at
     * startup, and only because the application asked for it.</p>
     *
     * @param properties     supplies {@code solace.schema-registry.registration.*}
     * @param resourceLoader resolves each declared schema's {@code location}
     * @param codecs         derives Avro and Protobuf schemas from a {@code topic-profile}
     *                       {@code payload-class} when {@code registration.include-topic-profile} is on
     * @return the registrar
     */
    @Bean
    @ConditionalOnMissingBean
    public SchemaArtifactRegistrar solaceSchemaArtifactRegistrar(SolaceProperties properties,
            ResourceLoader resourceLoader, SchemaCodecs codecs) {
        SchemaRegistrySettings settings = properties.getSchemaRegistry();
        SchemaArtifactRegistrar registrar = new SchemaArtifactRegistrar(settings, resourceLoader, codecs,
                getClass().getClassLoader());
        if (registrar.hasWork()) {
            String when = settings.getRegistration().getMode() == SchemaRegistrySettings.RegistrationMode.STARTUP
                    ? "at startup" : "on the first message";
            log.info("Apicurio Registry: {} declared and {} derived schema(s), published {}",
                    settings.getRegistration().getSchemas().size(),
                    registrar.hasDerivedSchemas()
                            ? settings.getTopicProfile().stream()
                                    .filter(m -> org.springframework.util.StringUtils.hasText(m.getPayloadClass()))
                                    .count()
                            : 0L,
                    when);
        }
        return registrar;
    }

    /**
     * The registry-aware converter, falling back to JSON over the application's {@code ObjectMapper} for
     * payloads the registry does not govern.
     *
     * @param codecs       the enabled formats' codecs
     * @param objectMapper the application's mapper when one exists
     * @param properties   supplies the governed destinations, strictness and per-topic POJO formats
     * @param registrar    publishes the declared schemas before the registry is first used
     * @return the converter every template, listener and request-reply template uses
     */
    @Bean
    @ConditionalOnMissingBean
    public SolaceMessageConverter solaceMessageConverter(SchemaCodecs codecs,
            ObjectProvider<ObjectMapper> objectMapper, SolaceProperties properties,
            SchemaArtifactRegistrar registrar) {
        SchemaRegistrySettings settings = properties.getSchemaRegistry();
        SchemaRegistrySolaceMessageConverter converter = new SchemaRegistrySolaceMessageConverter(codecs,
                objectMapper.getIfAvailable(ObjectMapper::new));
        converter.setDestinations(settings.getDestinations());
        converter.setRequireSchemaId(settings.isRequireSchemaId());
        Map<String, SchemaFormat> pojoFormats = new LinkedHashMap<>();
        for (SchemaRegistrySettings.TopicMapping mapping : settings.getTopicProfile()) {
            if (mapping.getFormat() != null) {
                pojoFormats.putIfAbsent(mapping.getTopicExpression(), mapping.getFormat());
            }
        }
        converter.setPojoFormats(pojoFormats);
        converter.setSchemaArtifactRegistrar(registrar);
        return converter;
    }

    /**
     * Rejects schema failures a retry cannot fix, when the application has no error handler of its own. To
     * combine with one, wrap it: {@code new SchemaRegistryErrorHandler(myHandler)}.
     *
     * @param properties supplies the listener's negative acknowledgement setting
     * @return the error handler given to the default listener container factory
     */
    @Bean
    @ConditionalOnMissingBean(SolaceListenerErrorHandler.class)
    public SchemaRegistryErrorHandler solaceSchemaRegistryErrorHandler(SolaceProperties properties) {
        if (!Boolean.TRUE.equals(properties.getListener().getNegativeAcknowledgement())) {
            log.warn("The schema registry error handler rejects messages that will never convert, but "
                    + "solace.listener.negative-acknowledgement is not true, so flows may not have negotiated the "
                    + "REJECTED outcome and such messages will be redelivered instead. Set it to true");
        }
        return new SchemaRegistryErrorHandler();
    }
}
