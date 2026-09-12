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
import org.springframework.util.StreamUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Publishes the schemas an application declares under {@code solace.schema-registry.registration.schemas}
 * to Apicurio Registry &mdash; either during application initialization or on the first message, as
 * {@code registration.mode} says.
 *
 * <p>This is the alternative to {@code auto-register}, which creates an artifact from the schema Apicurio
 * derived from the payload. That works for Avro (a schema from the class) and Protobuf (a schema from the
 * generated descriptor) but not for JSON Schema, which cannot be inferred from a POJO: for that format
 * either the artifact exists before the first message, or every serialisation fails. Declaring schemas
 * here also means the contract is a reviewed file in the repository rather than whatever the first
 * producer happened to send, which is what {@code auto-register} is unsuitable for in production.</p>
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
 * unchanged content on a restart finds the existing version rather than piling up new ones.</p>
 */
@Slf4j
public class SchemaArtifactRegistrar implements InitializingBean {

    private final SchemaRegistrySettings settings;

    private final ResourceLoader resourceLoader;

    private volatile boolean registered;

    /**
     * Create a registrar reading schema content through a {@link DefaultResourceLoader}.
     *
     * @param settings validated settings
     */
    public SchemaArtifactRegistrar(SchemaRegistrySettings settings) {
        this(settings, new DefaultResourceLoader());
    }

    /**
     * Create a registrar.
     *
     * @param settings       validated settings
     * @param resourceLoader resolves each schema's {@code location}
     */
    public SchemaArtifactRegistrar(SchemaRegistrySettings settings, ResourceLoader resourceLoader) {
        Assert.notNull(settings, "'settings' must not be null");
        Assert.notNull(resourceLoader, "'resourceLoader' must not be null");
        this.settings = settings;
        this.resourceLoader = resourceLoader;
    }

    /**
     * Whether this application declares any schema at all.
     *
     * @return {@code true} if there is something to publish
     */
    public boolean hasSchemas() {
        return !this.settings.getRegistration().getSchemas().isEmpty();
    }

    /**
     * Whether the declared schemas have been published in this instance.
     *
     * @return {@code true} once an attempt has succeeded, or when there is nothing to publish
     */
    public boolean isRegistered() {
        return this.registered || !hasSchemas();
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
     * Publish every declared schema, unless a previous attempt already succeeded.
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
                log.warn("Could not publish the declared schemas to Apicurio Registry at {}. Messages whose schema "
                        + "the registry cannot infer will fail until it succeeds; the next message tries again. Set "
                        + "solace.schema-registry.registration.fail-fast to make this stop the application instead",
                        this.settings.getUrl(), ex);
            }
        }
    }

    /**
     * Publish every declared schema, whether or not it has been done before.
     *
     * @throws SchemaRegistryConversionException if the registry rejects a schema or cannot be reached,
     *                                          classified so the error handler can tell a retryable failure
     *                                          from a permanent one
     */
    public void register() {
        List<SchemaRegistrySettings.DeclaredSchema> schemas = this.settings.getRegistration().getSchemas();
        if (schemas.isEmpty()) {
            return;
        }
        // Every schema is read first, so an unreadable location is reported without a registry connection.
        List<String> contents = new ArrayList<>(schemas.size());
        for (SchemaRegistrySettings.DeclaredSchema schema : schemas) {
            contents.add(read(schema));
        }
        SchemaRegistrySettings.IfArtifactExists ifExists = this.settings.getRegistration().getIfExists();
        String behaviour = (ifExists != null ? ifExists : SchemaRegistrySettings.IfArtifactExists.FIND_OR_CREATE_VERSION)
                .name();
        RegistryClientFacade client = RegistryClientFacadeFactory.create(
                new SchemaResolverConfig(ApicurioConfiguration.common(this.settings)));
        for (int i = 0; i < schemas.size(); i++) {
            publish(client, schemas.get(i), contents.get(i), behaviour);
        }
    }

    private void publish(RegistryClientFacade client, SchemaRegistrySettings.DeclaredSchema schema,
            String content, String behaviour) {
        try {
            RegistryVersionCoordinates coordinates = client.createSchema(schema.getFormat().getArtifactType(),
                    schema.getGroupId(), schema.getArtifactId(), schema.getVersion(), behaviour, false, content,
                    Set.of());
            log.info("Published the {} schema {}/{} from {} as version {}", schema.getFormat(),
                    coordinates.getGroupId(), coordinates.getArtifactId(), schema.getLocation(),
                    coordinates.getVersion());
        }
        catch (Exception ex) {
            throw SchemaRegistryConversionException.classify("Unable to publish the " + schema.getFormat()
                    + " schema '" + schema.getArtifactId() + "' from " + schema.getLocation()
                    + " to the registry", ex);
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
}
