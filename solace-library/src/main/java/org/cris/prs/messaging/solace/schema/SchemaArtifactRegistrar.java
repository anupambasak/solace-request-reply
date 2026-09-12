package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Publishes the schemas an application declares under {@code solace.schema-registry.registration.schemas},
 * and &mdash; when {@code registration.include-topic-profile} is on &mdash; schemas derived from the
 * {@code topic-profile} mappings that carry a {@code payload-class}, to Apicurio Registry. Either happens
 * during application initialization or on the first message, as {@code registration.mode} says.
 *
 * <p>This is the alternative to {@code auto-register}, which creates an artifact from the schema Apicurio
 * derived from the payload on the first message. Declaring or deriving schemas here registers them
 * <em>at initialization</em> instead, and &mdash; for {@code registration.schemas} &mdash; from a reviewed
 * file in the repository rather than whatever the first producer happened to send.</p>
 *
 * <h2>Two sources</h2>
 * <ul>
 *   <li>{@code registration.schemas} &mdash; a schema file (any format) read from a Spring resource
 *       location. The only way to get a JSON Schema into the registry, which cannot be inferred from a
 *       POJO.</li>
 *   <li>{@code topic-profile} mappings with a {@code payload-class}, when
 *       {@code registration.include-topic-profile} is on &mdash; an Avro or Protobuf schema
 *       {@linkplain SchemaCodec#deriveSchema(Class) derived from the class}. Mappings that resolve to JSON
 *       Schema, or that name no {@code payload-class}, are ignored. A declared schema wins over a derived
 *       one for the same artifact coordinates.</li>
 * </ul>
 *
 * <h2>When it runs</h2>
 * <ul>
 *   <li>{@code FIRST_MESSAGE} (the default) &mdash; {@link #registerOnce()} is called by
 *       {@link SchemaRegistrySolaceMessageConverter} immediately before the first serialisation or
 *       deserialisation that goes through the registry. Nothing contacts the registry at startup, so an
 *       instance starts while the registry is down, exactly as the serdes do.</li>
 *   <li>{@code STARTUP} &mdash; the same work runs from {@link #afterPropertiesSet()}, before anything is
 *       sent or received, so a missing or rejected schema is visible at boot rather than on the first
 *       request. The converter still guards every conversion, so a startup attempt that failed with
 *       {@code fail-fast} off is retried on the first message.</li>
 * </ul>
 *
 * <p>Registration happens once per application instance: the first attempt that succeeds sets the flag,
 * and every later call returns immediately. A failed attempt sets nothing, so the next call tries again
 * &mdash; which is what a registry that was briefly unreachable needs. With
 * {@code registration.fail-fast: true} the failure propagates instead: the context fails to start under
 * {@code STARTUP}, and the send or receive fails under {@code FIRST_MESSAGE}.</p>
 *
 * <p>Each schema is published with {@code ifExists} (default {@code FIND_OR_CREATE_VERSION}), so
 * unchanged content on a restart finds the existing version rather than piling up new ones. Deriving a
 * schema touches neither a message nor the registry, so only the publish step needs the registry to be
 * reachable.</p>
 */
@Slf4j
public class SchemaArtifactRegistrar implements InitializingBean {

    private final SchemaRegistrySettings settings;

    private final ResourceLoader resourceLoader;

    private final SchemaCodecs codecs;

    private final ClassLoader classLoader;

    private volatile boolean registered;

    /**
     * Create a registrar reading schema content through a {@link DefaultResourceLoader}, without codecs so
     * only {@code registration.schemas} are published.
     *
     * @param settings validated settings
     */
    public SchemaArtifactRegistrar(SchemaRegistrySettings settings) {
        this(settings, new DefaultResourceLoader(), null, null);
    }

    /**
     * Create a registrar without codecs, so only {@code registration.schemas} are published.
     *
     * @param settings       validated settings
     * @param resourceLoader resolves each schema's {@code location}
     */
    public SchemaArtifactRegistrar(SchemaRegistrySettings settings, ResourceLoader resourceLoader) {
        this(settings, resourceLoader, null, null);
    }

    /**
     * Create a registrar that can also derive {@code topic-profile} schemas.
     *
     * @param settings       validated settings
     * @param resourceLoader resolves each declared schema's {@code location}
     * @param codecs         the enabled codecs, used to derive Avro and Protobuf schemas from a payload
     *                       class; {@code null} disables {@code include-topic-profile}
     * @param classLoader    loads each mapping's {@code payload-class}; {@code null} uses the default
     *                       class loader
     */
    public SchemaArtifactRegistrar(SchemaRegistrySettings settings, ResourceLoader resourceLoader,
            SchemaCodecs codecs, ClassLoader classLoader) {
        Assert.notNull(settings, "'settings' must not be null");
        Assert.notNull(resourceLoader, "'resourceLoader' must not be null");
        this.settings = settings;
        this.resourceLoader = resourceLoader;
        this.codecs = codecs;
        this.classLoader = classLoader != null ? classLoader : ClassUtils.getDefaultClassLoader();
    }

    /**
     * Whether this application declares any schema file under {@code registration.schemas}.
     *
     * @return {@code true} if there is a declared schema to publish
     */
    public boolean hasSchemas() {
        return !this.settings.getRegistration().getSchemas().isEmpty();
    }

    /**
     * Whether this application derives any schema from a {@code topic-profile} mapping.
     *
     * @return {@code true} if {@code include-topic-profile} is on, codecs are available, and at least one
     *         mapping names a {@code payload-class}
     */
    public boolean hasDerivedSchemas() {
        if (!this.settings.getRegistration().isIncludeTopicProfile() || this.codecs == null) {
            return false;
        }
        return this.settings.getTopicProfile().stream()
                .anyMatch(mapping -> StringUtils.hasText(mapping.getPayloadClass()));
    }

    /**
     * Whether there is anything at all to publish, declared or derived.
     *
     * @return {@code true} if a call to {@link #register()} would publish something
     */
    public boolean hasWork() {
        return hasSchemas() || hasDerivedSchemas();
    }

    /**
     * Whether every schema this application publishes has been published in this instance.
     *
     * @return {@code true} once an attempt has succeeded, or when there is nothing to publish
     */
    public boolean isRegistered() {
        return this.registered || !hasWork();
    }

    /**
     * Publish at startup under {@code registration.mode: STARTUP}; do nothing otherwise.
     *
     * @throws RuntimeException if publishing fails and {@code registration.fail-fast} is set
     */
    @Override
    public void afterPropertiesSet() {
        if (this.settings.getRegistration().getMode() == SchemaRegistrySettings.RegistrationMode.STARTUP) {
            registerOnce();
        }
    }

    /**
     * Publish every schema, unless a previous attempt already succeeded.
     *
     * <p>Cheap to call on every message: once registration has succeeded this is a volatile read.</p>
     *
     * @throws RuntimeException if publishing fails and {@code registration.fail-fast} is set
     */
    public void registerOnce() {
        if (isRegistered()) {
            return;
        }
        synchronized (this) {
            if (this.registered) {
                return;
            }
            try {
                register();
                this.registered = true;
            }
            catch (RuntimeException ex) {
                if (this.settings.getRegistration().isFailFast()) {
                    throw ex;
                }
                log.warn("Could not publish the schemas to Apicurio Registry at {}. Messages whose schema the "
                        + "registry cannot infer will fail until it succeeds; the next message tries again. Set "
                        + "solace.schema-registry.registration.fail-fast to make this stop the application instead",
                        this.settings.getUrl(), ex);
            }
        }
    }

    /**
     * Publish every schema, declared and derived, whether or not it has been done before.
     *
     * @throws SchemaRegistryConversionException if a location or a payload class cannot be read, or the
     *                                          registry rejects a schema or cannot be reached, classified so
     *                                          the error handler can tell a retryable failure from a
     *                                          permanent one
     */
    public void register() {
        List<PendingSchema> pending = collectSchemas();
        if (pending.isEmpty()) {
            return;
        }
        SchemaRegistrySettings.IfArtifactExists ifExists = this.settings.getRegistration().getIfExists();
        String behaviour = (ifExists != null ? ifExists : SchemaRegistrySettings.IfArtifactExists.FIND_OR_CREATE_VERSION)
                .name();
        RegistryClientFacade client = RegistryClientFacadeFactory.create(
                new SchemaResolverConfig(ApicurioConfiguration.common(this.settings)));
        for (PendingSchema schema : pending) {
            publish(client, schema, behaviour);
        }
    }

    /**
     * Resolve every schema to publish &mdash; declared files read, {@code topic-profile} schemas derived
     * &mdash; without contacting the registry. Declared schemas come first and win over a derived schema
     * for the same artifact coordinates. Package-visible so the resolution can be tested without a registry.
     *
     * @return the schemas to publish, in order
     */
    List<PendingSchema> collectSchemas() {
        Map<String, PendingSchema> byCoordinates = new LinkedHashMap<>();
        for (SchemaRegistrySettings.DeclaredSchema declared : this.settings.getRegistration().getSchemas()) {
            PendingSchema schema = new PendingSchema(declared.getFormat(), declared.getGroupId(),
                    declared.getArtifactId(), declared.getVersion(), read(declared), declared.getLocation());
            byCoordinates.putIfAbsent(key(schema), schema);
        }
        if (hasDerivedSchemas()) {
            for (SchemaRegistrySettings.TopicMapping mapping : this.settings.getTopicProfile()) {
                PendingSchema derived = derive(mapping);
                if (derived != null) {
                    byCoordinates.putIfAbsent(key(derived), derived);
                }
            }
        }
        return new ArrayList<>(byCoordinates.values());
    }

    private PendingSchema derive(SchemaRegistrySettings.TopicMapping mapping) {
        if (!StringUtils.hasText(mapping.getPayloadClass())) {
            return null;
        }
        Class<?> payloadClass = loadPayloadClass(mapping.getPayloadClass());
        SchemaCodec codec = codecFor(mapping, payloadClass);
        if (codec == null || !codec.canDeriveSchema()) {
            log.debug("topic-profile mapping '{}' (artifact {}) resolves to a format whose schema cannot be "
                    + "derived from {}; skipping. Declare it under registration.schemas if it needs registering",
                    mapping.getTopicExpression(), mapping.getArtifactId(), mapping.getPayloadClass());
            return null;
        }
        String content = codec.deriveSchema(payloadClass);
        return new PendingSchema(codec.getFormat(), mapping.getGroupId(), mapping.getArtifactId(),
                mapping.getVersion(), content, "derived from " + mapping.getPayloadClass());
    }

    /**
     * The codec whose format a mapping's schema is derived in: the Avro codec when the mapping declares
     * {@code format: AVRO}, otherwise the codec whose format is specific to the payload class &mdash; a
     * Protobuf message, or an Avro generated record. A plain POJO with no {@code format} resolves to JSON
     * Schema, which has no codec here and returns {@code null}.
     */
    private SchemaCodec codecFor(SchemaRegistrySettings.TopicMapping mapping, Class<?> payloadClass) {
        if (mapping.getFormat() == SchemaFormat.AVRO) {
            return this.codecs.get(SchemaFormat.AVRO);
        }
        return this.codecs.forTargetType(payloadClass);
    }

    private Class<?> loadPayloadClass(String className) {
        try {
            return ClassUtils.forName(className, this.classLoader);
        }
        catch (ClassNotFoundException | LinkageError ex) {
            throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND,
                    "Unable to load the payload class '" + className + "' named by a "
                            + "solace.schema-registry.topic-profile mapping. Check the class name and that it is on "
                            + "the classpath", ex);
        }
    }

    private void publish(RegistryClientFacade client, PendingSchema schema, String behaviour) {
        try {
            RegistryVersionCoordinates coordinates = client.createSchema(schema.format().getArtifactType(),
                    schema.groupId(), schema.artifactId(), schema.version(), behaviour, false, schema.content(),
                    Set.of());
            log.info("Published the {} schema {}/{} ({}) as version {}", schema.format(),
                    coordinates.getGroupId(), coordinates.getArtifactId(), schema.source(),
                    coordinates.getVersion());
        }
        catch (Exception ex) {
            throw SchemaRegistryConversionException.classify("Unable to publish the " + schema.format()
                    + " schema '" + schema.artifactId() + "' (" + schema.source() + ") to the registry", ex);
        }
    }

    private String read(SchemaRegistrySettings.DeclaredSchema schema) {
        Resource resource = this.resourceLoader.getResource(schema.getLocation());
        try (InputStream in = resource.getInputStream()) {
            return StreamUtils.copyToString(in, StandardCharsets.UTF_8);
        }
        catch (Exception ex) {
            throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND,
                    "Unable to read the schema for artifact '" + schema.getArtifactId() + "' from "
                            + schema.getLocation() + ". Check solace.schema-registry.registration.schemas", ex);
        }
    }

    private static String key(PendingSchema schema) {
        return (schema.groupId() == null ? "" : schema.groupId()) + '/' + schema.artifactId();
    }

    /**
     * A schema resolved and ready to publish: its format, coordinates, content, and a human-readable note
     * of where the content came from (a resource location, or the class it was derived from).
     *
     * @param format     the schema language and Apicurio artifact type
     * @param groupId    the artifact group, or {@code null} for the registry's default
     * @param artifactId the artifact id
     * @param version    the artifact version, or {@code null} to let the registry assign one
     * @param content    the schema content
     * @param source     where the content came from, for logging and error messages
     */
    record PendingSchema(SchemaFormat format, String groupId, String artifactId, String version, String content,
            String source) {
    }
}
