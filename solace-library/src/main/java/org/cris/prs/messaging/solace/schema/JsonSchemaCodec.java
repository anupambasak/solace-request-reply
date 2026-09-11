package org.cris.prs.messaging.solace.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaDeserializer;
import io.apicurio.registry.serde.jsonschema.JsonSchemaSerializer;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * JSON Schema through Apicurio Registry.
 *
 * <p>Serialises POJOs and {@code JsonNode}s with the <em>application's</em> {@code ObjectMapper}, so the
 * JSON on the wire is exactly what the plain JSON converter would have written &mdash; naming strategy,
 * modules, dates &mdash; with schema validation added. Deserialises to a {@code JsonNode}, which
 * {@link SchemaRegistrySolaceMessageConverter} maps onto each listener's own type with the same mapper; so
 * one validating deserializer serves every listener, no schema needs a {@code javaType}, and payload
 * classes need no schema dependency at all. A schema that does carry {@code javaType} makes Apicurio
 * return that class instead, which the converter accepts when it is what the listener asked for.</p>
 *
 * <p>Needs {@code io.apicurio:apicurio-registry-serde-common-jsonschema} on the classpath.</p>
 */
public class JsonSchemaCodec extends ApicurioSchemaCodec {

    private final ObjectMapper objectMapper;

    private final Lazy<JsonSchemaSerializer<Object>> serializer = new Lazy<>(this::createSerializer);

    private final Lazy<JsonSchemaDeserializer<Object>> deserializer = new Lazy<>(this::createDeserializer);

    /**
     * Create a codec. Nothing contacts the registry until the first message.
     *
     * @param settings     validated settings
     * @param objectMapper the application's mapper, used on both sides
     */
    public JsonSchemaCodec(SchemaRegistrySettings settings, ObjectMapper objectMapper) {
        super(settings);
        Assert.notNull(objectMapper, "'objectMapper' must not be null");
        this.objectMapper = objectMapper;
    }

    /** {@inheritDoc} */
    @Override
    public SchemaFormat getFormat() {
        return SchemaFormat.JSON_SCHEMA;
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code true} for a Jackson {@code JsonNode}. A POJO is not a schema value; the converter decides
     * by destination whether to send it through this codec.</p>
     */
    @Override
    public boolean isSchemaPayload(Object payload) {
        return payload instanceof JsonNode;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Always {@code false}: JSON Schema can produce any type, so it is the converter's default for a
     * framed message whose format is not otherwise known, rather than a match by type.</p>
     */
    @Override
    public boolean producesType(Class<?> targetType) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] serialize(String destinationName, Object payload) {
        return this.serializer.get().serializeData(destinationName, payload);
    }

    /** {@inheritDoc} */
    @Override
    public Object deserialize(String destinationName, byte[] body, Class<?> targetType) {
        return this.deserializer.get().deserializeData(destinationName, body);
    }

    private JsonSchemaSerializer<Object> createSerializer() {
        JsonSchemaSerializer<Object> serializer = new JsonSchemaSerializer<>();
        serializer.setObjectMapper(this.objectMapper);
        return configured(serializer, configuration(jsonSchemaProperties(), this.settings.getJsonSchema().getProperties()));
    }

    private JsonSchemaDeserializer<Object> createDeserializer() {
        JsonSchemaDeserializer<Object> deserializer = new JsonSchemaDeserializer<>();
        deserializer.setObjectMapper(this.objectMapper);
        return configured(deserializer, configuration(jsonSchemaProperties(), this.settings.getJsonSchema().getProperties()));
    }

    private Map<String, Object> jsonSchemaProperties() {
        Map<String, Object> json = new HashMap<>();
        SchemaRegistrySettings.JsonSchema settings = this.settings.getJsonSchema();
        ApicurioConfiguration.putIfSet(json, SerdeConfig.VALIDATION_ENABLED, settings.getValidation());
        ApicurioConfiguration.putIfSet(json, SchemaResolverConfig.SCHEMA_LOCATION, settings.getSchemaLocation());
        return json;
    }
}
