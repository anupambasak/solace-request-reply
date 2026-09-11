package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Translates {@link SchemaRegistrySettings} into the Apicurio configuration shared by every serializer
 * and deserializer, whatever the format.
 *
 * <p>Only values that were set are written, so Apicurio keeps its own default for everything else. Raw
 * {@code properties} are applied last and override the rest.</p>
 */
final class ApicurioConfiguration {

    /** Apicurio's Avro record-name strategy; named rather than referenced so this class needs no Avro jar. */
    static final String RECORD_ID_STRATEGY = "io.apicurio.registry.serde.avro.strategy.RecordIdStrategy";

    private ApicurioConfiguration() {
    }

    /**
     * Build the configuration common to every format.
     *
     * @param settings the bound settings
     * @return a new, mutable configuration map &mdash; Apicurio writes into it while configuring
     */
    static Map<String, Object> common(SchemaRegistrySettings settings) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverConfig.REGISTRY_URL, settings.getUrl());
        putIfSet(config, SchemaResolverConfig.AUTH_USERNAME, settings.getUsername());
        putIfSet(config, SchemaResolverConfig.AUTH_PASSWORD, settings.getPassword());

        SchemaRegistrySettings.OAuth oauth = settings.getOauth();
        putIfSet(config, SchemaResolverConfig.AUTH_TOKEN_ENDPOINT, oauth.getTokenEndpoint());
        putIfSet(config, SchemaResolverConfig.AUTH_CLIENT_ID, oauth.getClientId());
        putIfSet(config, SchemaResolverConfig.AUTH_CLIENT_SECRET, oauth.getClientSecret());
        putIfSet(config, SchemaResolverConfig.AUTH_CLIENT_SCOPE, oauth.getScope());

        SchemaRegistrySettings.Tls tls = settings.getTls();
        putIfSet(config, SchemaResolverConfig.TLS_TRUSTSTORE_LOCATION, tls.getTruststoreLocation());
        putIfSet(config, SchemaResolverConfig.TLS_TRUSTSTORE_PASSWORD, tls.getTruststorePassword());
        putIfSet(config, SchemaResolverConfig.TLS_TRUSTSTORE_TYPE, tls.getTruststoreType());
        putIfSet(config, SchemaResolverConfig.TLS_KEYSTORE_LOCATION, tls.getKeystoreLocation());
        putIfSet(config, SchemaResolverConfig.TLS_KEYSTORE_PASSWORD, tls.getKeystorePassword());
        putIfSet(config, SchemaResolverConfig.TLS_KEYSTORE_TYPE, tls.getKeystoreType());
        putIfSet(config, SchemaResolverConfig.TLS_TRUST_ALL, tls.getTrustAll());
        putIfSet(config, SchemaResolverConfig.TLS_VERIFY_HOST, tls.getVerifyHost());
        if (settings.getHttpAdapter() != null) {
            config.put(SchemaResolverConfig.HTTP_ADAPTER, settings.getHttpAdapter().name());
        }

        applyStrategy(config, settings);

        putIfSet(config, SchemaResolverConfig.FIND_LATEST_ARTIFACT, settings.getFindLatest());
        SchemaRegistrySettings.ExplicitArtifact explicit = settings.getExplicitArtifact();
        putIfSet(config, SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID, explicit.getGroupId());
        putIfSet(config, SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, explicit.getArtifactId());
        putIfSet(config, SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION, explicit.getVersion());
        putIfSet(config, SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, settings.getAutoRegister());
        if (settings.getAutoRegisterIfExists() != null) {
            config.put(SchemaResolverConfig.AUTO_REGISTER_ARTIFACT_IF_EXISTS, settings.getAutoRegisterIfExists().name());
        }
        if (settings.getUseId() != null) {
            config.put(SerdeConfig.USE_ID,
                    settings.getUseId() == SchemaRegistrySettings.IdOption.GLOBAL_ID ? "globalId" : "contentId");
        }
        putIfSet(config, SchemaResolverConfig.DEREFERENCE_SCHEMA, settings.getDereferenceSchema());

        SchemaRegistrySettings.Cache cache = settings.getCache();
        if (cache.getCheckPeriod() != null) {
            config.put(SchemaResolverConfig.CHECK_PERIOD_MS, cache.getCheckPeriod().toMillis());
        }
        putIfSet(config, SchemaResolverConfig.CACHE_LATEST, cache.getLatest());
        putIfSet(config, SchemaResolverConfig.FAULT_TOLERANT_REFRESH, cache.getFaultTolerantRefresh());
        putIfSet(config, SchemaResolverConfig.BACKGROUND_REFRESH_ENABLED, cache.getBackgroundRefresh());

        SchemaRegistrySettings.Retry retry = settings.getRetry();
        putIfSet(config, SchemaResolverConfig.RETRY_COUNT, retry.getCount());
        if (retry.getBackoff() != null) {
            config.put(SchemaResolverConfig.RETRY_BACKOFF_MS, retry.getBackoff().toMillis());
        }

        config.putAll(settings.getProperties());
        return config;
    }

    /**
     * The common configuration, then a format's own keys, then that format's raw properties.
     *
     * @param settings       the bound settings
     * @param formatSpecific the format's own keys
     * @param formatRaw      the format's raw pass-through properties
     * @return the complete configuration for one serializer or deserializer
     */
    static Map<String, Object> forFormat(SchemaRegistrySettings settings, Map<String, Object> formatSpecific,
            Map<String, String> formatRaw) {
        Map<String, Object> config = common(settings);
        config.putAll(formatSpecific);
        config.putAll(formatRaw);
        return config;
    }

    private static void applyStrategy(Map<String, Object> config, SchemaRegistrySettings settings) {
        String strategy = settings.getArtifactResolverStrategy();
        if (!StringUtils.hasText(strategy) || SchemaRegistrySettings.STRATEGY_TOPIC_PROFILE.equalsIgnoreCase(strategy)) {
            if (!settings.getTopicProfile().isEmpty()) {
                config.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY,
                        new SolaceTopicProfileStrategy<>(settings.getTopicProfile()));
            }
            // With no mappings validate() has required an explicit artifact, which overrides any strategy.
        }
        else if (SchemaRegistrySettings.STRATEGY_DESTINATION.equalsIgnoreCase(strategy)) {
            config.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, SimpleTopicIdStrategy.class.getName());
        }
        else if (SchemaRegistrySettings.STRATEGY_TOPIC.equalsIgnoreCase(strategy)) {
            config.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getName());
        }
        else if (SchemaRegistrySettings.STRATEGY_RECORD.equalsIgnoreCase(strategy)) {
            config.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, RECORD_ID_STRATEGY);
        }
        else {
            config.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, strategy);
        }
    }

    static void putIfSet(Map<String, Object> config, String key, Object value) {
        if (value instanceof String text ? StringUtils.hasText(text) : value != null) {
            config.put(key, value);
        }
    }
}
