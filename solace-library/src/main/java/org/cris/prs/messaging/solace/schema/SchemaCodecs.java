package org.cris.prs.messaging.solace.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The enabled {@link SchemaCodec}s, one per {@link SchemaFormat}, and the factory that creates them.
 *
 * <p>Closing this closes every codec. The auto-configuration registers it as a bean, so Spring does that
 * on shutdown.</p>
 */
@Slf4j
public final class SchemaCodecs implements AutoCloseable {

    private final Map<SchemaFormat, SchemaCodec> codecs;

    private SchemaCodecs(Collection<? extends SchemaCodec> codecs) {
        Map<SchemaFormat, SchemaCodec> byFormat = new EnumMap<>(SchemaFormat.class);
        for (SchemaCodec codec : codecs) {
            Assert.notNull(codec, "a codec must not be null");
            if (byFormat.putIfAbsent(codec.getFormat(), codec) != null) {
                throw new IllegalArgumentException("Two codecs for format " + codec.getFormat());
            }
        }
        Assert.isTrue(!byFormat.isEmpty(), "at least one schema codec is required");
        this.codecs = Collections.unmodifiableMap(byFormat);
    }

    /**
     * Wrap codecs created elsewhere &mdash; a custom codec, or a fake in a test.
     *
     * @param codecs at most one per format; at least one
     * @return the set
     */
    public static SchemaCodecs of(SchemaCodec... codecs) {
        return new SchemaCodecs(List.of(codecs));
    }

    /**
     * Validate the settings and create a codec for every enabled format.
     *
     * @param settings     the bound settings
     * @param objectMapper the application's mapper, used by the JSON Schema codec
     * @param classLoader  where to look for the Apicurio serde modules; may be {@code null}
     * @return unopened codecs; nothing contacts the registry until the first message
     * @throws IllegalStateException if the settings are invalid, a listed format's module is missing, or no
     *                               format's module is present, naming the fix
     */
    public static SchemaCodecs create(SchemaRegistrySettings settings, ObjectMapper objectMapper,
            ClassLoader classLoader) {
        settings.validate();
        List<SchemaFormat> requested = settings.getFormats();
        List<SchemaFormat> enabled = new ArrayList<>();
        if (requested.isEmpty()) {
            for (SchemaFormat format : SchemaFormat.values()) {
                if (isPresent(format, classLoader)) {
                    enabled.add(format);
                }
            }
            if (enabled.isEmpty()) {
                throw new IllegalStateException("solace.schema-registry.url is set but no Apicurio serde module is "
                        + "on the classpath. Add one or more of " + SchemaFormat.AVRO.getArtifact() + ", "
                        + SchemaFormat.PROTOBUF.getArtifact() + ", " + SchemaFormat.JSON_SCHEMA.getArtifact());
            }
        }
        else {
            for (SchemaFormat format : requested) {
                if (!isPresent(format, classLoader)) {
                    throw new IllegalStateException("solace.schema-registry.formats includes " + format + " but "
                            + format.getArtifact() + " is not on the classpath. Add it, or remove the format");
                }
                if (!enabled.contains(format)) {
                    enabled.add(format);
                }
            }
        }
        List<SchemaCodec> codecs = new ArrayList<>();
        for (SchemaFormat format : enabled) {
            codecs.add(switch (format) {
                case AVRO -> new AvroSchemaCodec(settings);
                case PROTOBUF -> new ProtobufSchemaCodec(settings);
                case JSON_SCHEMA -> new JsonSchemaCodec(settings, objectMapper);
            });
        }
        log.info("Apicurio Registry formats enabled: {}", enabled);
        return new SchemaCodecs(codecs);
    }

    private static boolean isPresent(SchemaFormat format, ClassLoader classLoader) {
        return ClassUtils.isPresent(format.getSerializerClassName(), classLoader);
    }

    /**
     * The codec for a format.
     *
     * @param format the format
     * @return the codec, or {@code null} if the format is not enabled
     */
    public SchemaCodec get(SchemaFormat format) {
        return this.codecs.get(format);
    }

    /**
     * Every enabled codec, in {@link SchemaFormat} declaration order.
     *
     * @return the codecs; never empty
     */
    public Collection<SchemaCodec> all() {
        return this.codecs.values();
    }

    /**
     * The codec that serialises a payload as it is: the one whose {@link SchemaCodec#isSchemaPayload}
     * accepts it.
     *
     * @param payload the outbound payload
     * @return the codec, or {@code null} if no format recognises the payload's type
     */
    public SchemaCodec forPayload(Object payload) {
        for (SchemaCodec codec : this.codecs.values()) {
            if (codec.isSchemaPayload(payload)) {
                return codec;
            }
        }
        return null;
    }

    /**
     * The codec whose format is specific to a listener type: an Avro record type, a Protobuf message type.
     *
     * @param targetType the type a listener asked for
     * @return the codec, or {@code null} if the type is not specific to any enabled format
     */
    public SchemaCodec forTargetType(Class<?> targetType) {
        for (SchemaCodec codec : this.codecs.values()) {
            if (codec.producesType(targetType)) {
                return codec;
            }
        }
        return null;
    }

    /** Close every codec. Idempotent, because every codec's {@code close} is. */
    @Override
    public void close() {
        for (SchemaCodec codec : this.codecs.values()) {
            try {
                codec.close();
            }
            catch (Exception ex) {
                log.warn("Failed to close the {} schema codec", codec.getFormat(), ex);
            }
        }
    }
}
