package org.cris.prs.messaging.solace.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.core.DefaultSolaceHeaderMapper;
import org.cris.prs.messaging.solace.core.JacksonSolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceMessageConverter;
import org.cris.prs.messaging.solace.support.SolaceTopicMatcher;
import org.springframework.util.Assert;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SolaceMessageConverter} that serialises and validates payloads against schemas held in Apicurio
 * Registry &mdash; Avro, Protobuf and JSON Schema &mdash; falling back to another converter (plain JSON by
 * default) for everything the registry does not govern.
 *
 * <h2>Outbound</h2>
 * <ol>
 *   <li>{@code null}, {@code byte[]} and {@code String} payloads go to the fallback, as they would without a
 *       registry.</li>
 *   <li>A payload one format recognises natively &mdash; an Avro record, a Protobuf message, a
 *       {@code JsonNode} &mdash; goes through that format's codec, whatever its destination.</li>
 *   <li>Any other payload goes through the registry only if its destination matches one of the
 *       {@code destinations} expressions (all destinations when there are none), in the format the
 *       {@linkplain #setPojoFormats POJO format} for that destination names &mdash; JSON Schema by default,
 *       or Avro by reflection. A format that is not enabled, or cannot write POJOs, is a
 *       {@link SchemaRegistryConversionException.Reason#TYPE_MISMATCH}.</li>
 * </ol>
 * <p>The body carries Apicurio's standard framing &mdash; magic byte, schema id, encoded payload &mdash;
 * and the format is written as the {@value SchemaRegistryHeaders#SCHEMA_FORMAT} user property.</p>
 *
 * <h2>Inbound</h2>
 * <ol>
 *   <li>A {@code null}, {@code Object}, {@code BytesXMLMessage}, {@code byte[]} or {@code String} target
 *       keeps its fallback behaviour &mdash; raw message or raw body, no registry call.</li>
 *   <li>A registry-framed body is decoded through the registry. The codec is chosen by the
 *       {@value SchemaRegistryHeaders#SCHEMA_FORMAT} property; failing that, by the listener's type (an Avro
 *       record or Protobuf message type); failing that, JSON Schema if enabled, or the only enabled format.
 *       A JSON Schema result is a {@code JsonNode}, mapped onto the listener's type with the application's
 *       {@code ObjectMapper}.</li>
 *   <li>Any other body goes to the fallback &mdash; unless {@code requireSchemaId} is set and the
 *       destination is governed, in which case it is a
 *       {@link SchemaRegistryConversionException.Reason#MISSING_SCHEMA_ID}.</li>
 * </ol>
 *
 * <p>This converter does not close its codecs; whoever created them owns them. The auto-configuration
 * registers {@link SchemaCodecs} as a bean, which Spring closes on shutdown.</p>
 */
@Slf4j
public class SchemaRegistrySolaceMessageConverter implements SolaceMessageConverter {

    private final SchemaCodecs codecs;

    private final ObjectMapper objectMapper;

    private final SolaceMessageConverter fallback;

    private List<String> destinations = List.of();

    private boolean requireSchemaId;

    private Map<String, SchemaFormat> pojoFormats = Map.of();

    private SchemaArtifactRegistrar registrar;

    /**
     * Create a converter falling back to {@link JacksonSolaceMessageConverter} over the same mapper.
     *
     * @param codecs       the enabled formats' codecs
     * @param objectMapper maps decoded JSON onto listener types, and backs the JSON fallback
     */
    public SchemaRegistrySolaceMessageConverter(SchemaCodecs codecs, ObjectMapper objectMapper) {
        this(codecs, objectMapper, new JacksonSolaceMessageConverter(objectMapper));
    }

    /**
     * Create a converter.
     *
     * @param codecs       the enabled formats' codecs
     * @param objectMapper maps decoded JSON onto listener types
     * @param fallback     converts everything the registry does not govern
     */
    public SchemaRegistrySolaceMessageConverter(SchemaCodecs codecs, ObjectMapper objectMapper,
            SolaceMessageConverter fallback) {
        Assert.notNull(codecs, "'codecs' must not be null");
        Assert.notNull(objectMapper, "'objectMapper' must not be null");
        Assert.notNull(fallback, "'fallback' must not be null");
        this.codecs = codecs;
        this.objectMapper = objectMapper;
        this.fallback = fallback;
    }

    /**
     * Restrict registry governance of POJO payloads to destinations matching these Solace topic expressions.
     *
     * @param destinations topic expressions, wildcards allowed; {@code null} or empty governs every
     *                     destination
     */
    public void setDestinations(List<String> destinations) {
        this.destinations = destinations == null ? List.of() : List.copyOf(destinations);
    }

    /**
     * Reject, rather than fall back on, a message on a governed destination whose body is not
     * registry-framed.
     *
     * @param requireSchemaId {@code true} for strict inbound conversion
     */
    public void setRequireSchemaId(boolean requireSchemaId) {
        this.requireSchemaId = requireSchemaId;
    }

    /**
     * Choose, per destination, the format POJO payloads are written in.
     *
     * @param pojoFormats Solace topic expression to format, tried in iteration order, first match wins;
     *                    a destination no expression matches uses JSON Schema. {@code null} clears it
     */
    public void setPojoFormats(Map<String, SchemaFormat> pojoFormats) {
        this.pojoFormats = pojoFormats == null ? Map.of() : new LinkedHashMap<>(pojoFormats);
    }

    /**
     * The registrar that publishes this application's declared schemas, called immediately before the first
     * conversion that goes through the registry.
     *
     * <p>Optional: without one, schemas are whatever {@code auto-register} or an external process put in
     * the registry. The registrar is cheap to call once it has succeeded, and a no-op when nothing is
     * declared.</p>
     *
     * @param registrar the registrar, or {@code null} for none
     */
    public void setSchemaArtifactRegistrar(SchemaArtifactRegistrar registrar) {
        this.registrar = registrar;
    }

    /**
     * The format POJO payloads sent to a destination are written in.
     *
     * @param destinationName the topic or queue name; may be {@code null}
     * @return the first matching {@linkplain #setPojoFormats POJO format}, or {@code JSON_SCHEMA}
     */
    public SchemaFormat pojoFormatFor(String destinationName) {
        if (destinationName != null) {
            for (Map.Entry<String, SchemaFormat> entry : this.pojoFormats.entrySet()) {
                if (SolaceTopicMatcher.matches(entry.getKey(), destinationName)) {
                    return entry.getValue();
                }
            }
        }
        return SchemaFormat.JSON_SCHEMA;
    }

    /**
     * The codecs this converter delegates serde work to.
     *
     * @return the codecs
     */
    public SchemaCodecs getCodecs() {
        return this.codecs;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Without a destination only native schema values &mdash; and, when no {@code destinations} are
     * configured, every payload &mdash; go through the registry.</p>
     */
    @Override
    public XMLMessage toMessage(Object payload) {
        return toMessage(payload, null);
    }

    /** {@inheritDoc} */
    @Override
    public XMLMessage toMessage(Object payload, String destination) {
        if (payload == null || payload instanceof byte[] || payload instanceof String) {
            return this.fallback.toMessage(payload, destination);
        }
        String name = destination != null ? DefaultSolaceHeaderMapper.toDestination(destination).getName() : null;
        SchemaCodec codec = this.codecs.forPayload(payload);
        if (codec == null) {
            if (!isGoverned(name)) {
                return this.fallback.toMessage(payload, destination);
            }
            SchemaFormat format = pojoFormatFor(name);
            codec = this.codecs.get(format);
            if (codec == null || !codec.acceptsPojos()) {
                throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.TYPE_MISMATCH,
                        "Destination '" + name + "' is governed by the schema registry, and POJOs sent to it are "
                                + "written as " + format + ", but " + (codec == null ? format + " is not enabled"
                                : "the " + format + " codec cannot write a " + payload.getClass().getName())
                                + ". Enable the format (Avro needs solace.schema-registry.avro.datum-provider: "
                                + "REFLECT), send a native Avro record or Protobuf message, or exclude the "
                                + "destination from solace.schema-registry.destinations", null);
            }
        }
        ensureSchemasRegistered();
        byte[] body;
        try {
            body = codec.serialize(name, payload);
        }
        catch (Exception ex) {
            throw SchemaRegistryConversionException.classify("Unable to serialize a " + payload.getClass().getName()
                    + " for '" + name + "' as " + codec.getFormat(), ex);
        }
        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        message.setData(body);
        try {
            SDTMap properties = JCSMPFactory.onlyInstance().createMap();
            properties.putString(SchemaRegistryHeaders.SCHEMA_FORMAT, codec.getFormat().getArtifactType());
            message.setProperties(properties);
        }
        catch (Exception ex) {
            log.debug("Unable to write the {} user property", SchemaRegistryHeaders.SCHEMA_FORMAT, ex);
        }
        return message;
    }

    /** {@inheritDoc} */
    @Override
    public Object fromMessage(BytesXMLMessage message, Class<?> targetType) {
        if (targetType == null || Object.class.equals(targetType)
                || BytesXMLMessage.class.isAssignableFrom(targetType)
                || byte[].class.equals(targetType) || String.class.equals(targetType)) {
            return this.fallback.fromMessage(message, targetType);
        }
        String name = message.getDestination() != null ? message.getDestination().getName() : null;
        byte[] body = JacksonSolaceMessageConverter.bodyOf(message);
        if (!SchemaRegistryHeaders.isFramed(body)) {
            if (this.requireSchemaId && isGoverned(name)) {
                throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.MISSING_SCHEMA_ID,
                        "Message on '" + name + "' is not registry-framed, and "
                                + "solace.schema-registry.require-schema-id is on", null);
            }
            return this.fallback.fromMessage(message, targetType);
        }
        SchemaCodec codec = codecFor(message, targetType, name);
        ensureSchemasRegistered();
        Object value;
        try {
            value = codec.deserialize(name, body, targetType);
        }
        catch (Exception ex) {
            throw SchemaRegistryConversionException.classify("Unable to deserialize the " + codec.getFormat()
                    + " message on '" + name + "' into " + targetType.getName(), ex);
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        if (value instanceof JsonNode node) {
            try {
                return this.objectMapper.treeToValue(node, targetType);
            }
            catch (Exception ex) {
                throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.TYPE_MISMATCH,
                        "Message on '" + name + "' is valid against its schema but cannot be mapped onto "
                                + targetType.getName(), ex);
            }
        }
        throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.TYPE_MISMATCH,
                "Message on '" + name + "' decoded to " + (value != null ? value.getClass().getName() : "null")
                        + ", not the " + targetType.getName() + " the listener asked for", null);
    }

    /**
     * Whether a destination's POJO payloads are governed by the registry.
     *
     * @param destinationName the topic or queue name; may be {@code null}
     * @return {@code true} when no {@code destinations} are configured, or one of them matches
     */
    public boolean isGoverned(String destinationName) {
        if (this.destinations.isEmpty()) {
            return true;
        }
        if (destinationName == null) {
            return false;
        }
        for (String expression : this.destinations) {
            if (SolaceTopicMatcher.matches(expression, destinationName)) {
                return true;
            }
        }
        return false;
    }

    private SchemaCodec codecFor(BytesXMLMessage message, Class<?> targetType, String name) {
        String declared = readFormat(message);
        if (declared != null) {
            SchemaFormat format = SchemaFormat.fromArtifactType(declared);
            SchemaCodec codec = format != null ? this.codecs.get(format) : null;
            if (codec == null) {
                throw new SchemaRegistryConversionException(
                        SchemaRegistryConversionException.Reason.UNSUPPORTED_FORMAT,
                        "Message on '" + name + "' is " + declared + ", which is not enabled in "
                                + "solace.schema-registry.formats", null);
            }
            return codec;
        }
        SchemaCodec byType = this.codecs.forTargetType(targetType);
        if (byType != null) {
            return byType;
        }
        SchemaCodec json = this.codecs.get(SchemaFormat.JSON_SCHEMA);
        if (json != null) {
            return json;
        }
        if (this.codecs.all().size() == 1) {
            return this.codecs.all().iterator().next();
        }
        throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.UNSUPPORTED_FORMAT,
                "Message on '" + name + "' is registry-framed but carries no " + SchemaRegistryHeaders.SCHEMA_FORMAT
                        + " property, and " + targetType.getName() + " does not identify a format", null);
    }

    private static String readFormat(BytesXMLMessage message) {
        SDTMap properties = message.getProperties();
        if (properties == null) {
            return null;
        }
        try {
            return properties.containsKey(SchemaRegistryHeaders.SCHEMA_FORMAT)
                    ? properties.getString(SchemaRegistryHeaders.SCHEMA_FORMAT) : null;
        }
        catch (Exception ex) {
            log.debug("Unable to read the {} user property", SchemaRegistryHeaders.SCHEMA_FORMAT, ex);
            return null;
        }
    }

    /**
     * Publish the declared schemas if they are not in the registry yet, before the registry is first used.
     *
     * <p>Under {@code registration.mode: STARTUP} this has already happened, so it is a flag read; under
     * {@code FIRST_MESSAGE} this is the moment it happens.</p>
     */
    private void ensureSchemasRegistered() {
        if (this.registrar != null) {
            this.registrar.registerOnce();
        }
    }
}
