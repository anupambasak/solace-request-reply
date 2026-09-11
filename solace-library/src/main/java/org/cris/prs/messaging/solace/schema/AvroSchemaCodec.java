package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.serde.avro.AvroDeserializer;
import io.apicurio.registry.serde.avro.AvroSerdeConfig;
import io.apicurio.registry.serde.avro.AvroSerializer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Apache Avro through Apicurio Registry.
 *
 * <p>Serialises any Avro record &mdash; {@code GenericRecord} or a generated {@code SpecificRecord}.
 * Deserialises into whichever the listener asks for: a {@code SpecificRecord} subtype gets a deserializer
 * with Apicurio's specific reader, anything else a {@code GenericRecord}. The two deserializers are created
 * independently, on first need.</p>
 *
 * <p>With {@code avro.datum-provider: REFLECT} (or {@code REFLECT_ALLOW_NULL}) it also writes and reads
 * plain Java objects, by Avro reflection: the schema is derived from the class's fields, and a reader
 * instantiates the class the schema names. That is what lets a shared DTO travel as Avro unchanged.</p>
 *
 * <p>Avro only loads classes it trusts. The class of every payload sent and every listener type is trusted
 * automatically, and {@code avro.trusted-packages} adds packages for nested field types; see
 * {@code AvroClassTrust}.</p>
 *
 * <p>Needs {@code io.apicurio:apicurio-registry-serde-common-avro} on the classpath.</p>
 */
public class AvroSchemaCodec extends ApicurioSchemaCodec {

    private final Lazy<AvroSerializer<Object>> serializer = new Lazy<>(this::createSerializer);

    private final Lazy<AvroDeserializer<Object>> genericDeserializer = new Lazy<>(() -> createDeserializer(false));

    private final Lazy<AvroDeserializer<Object>> specificDeserializer = new Lazy<>(() -> createDeserializer(true));

    /**
     * Create a codec. Nothing contacts the registry until the first message.
     *
     * @param settings validated settings
     */
    public AvroSchemaCodec(SchemaRegistrySettings settings) {
        super(settings);
        AvroClassTrust.trustPackages(settings.getAvro().getTrustedPackages());
    }

    /** {@inheritDoc} */
    @Override
    public SchemaFormat getFormat() {
        return SchemaFormat.AVRO;
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code true} for any Avro {@code GenericContainer}: generic and specific records alike.</p>
     */
    @Override
    public boolean isSchemaPayload(Object payload) {
        return payload instanceof GenericContainer;
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code true} with a reflect datum provider.</p>
     */
    @Override
    public boolean acceptsPojos() {
        return this.settings.getAvro().isReflect();
    }

    /** {@inheritDoc} */
    @Override
    public boolean producesType(Class<?> targetType) {
        return targetType != null && GenericContainer.class.isAssignableFrom(targetType);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] serialize(String destinationName, Object payload) {
        // Avro only loads classes it trusts; the payload's class is one the application is already using.
        AvroClassTrust.trust(payload.getClass());
        return this.serializer.get().serializeData(destinationName, payload);
    }

    /** {@inheritDoc} */
    @Override
    public Object deserialize(String destinationName, byte[] body, Class<?> targetType) {
        // A reader instantiates the class the schema names; the listener's own type is safe to trust.
        AvroClassTrust.trust(targetType);
        boolean specific = !this.settings.getAvro().isReflect()
                && targetType != null && SpecificRecord.class.isAssignableFrom(targetType);
        AvroDeserializer<Object> deserializer = specific ? this.specificDeserializer.get() : this.genericDeserializer.get();
        return deserializer.deserializeData(destinationName, body);
    }

    private AvroSerializer<Object> createSerializer() {
        return configured(new AvroSerializer<>(), configuration(avroProperties(), this.settings.getAvro().getProperties()));
    }

    private AvroDeserializer<Object> createDeserializer(boolean specific) {
        Map<String, Object> avro = avroProperties();
        avro.put(AvroSerdeConfig.USE_SPECIFIC_AVRO_READER, specific);
        return configured(new AvroDeserializer<>(), configuration(avro, this.settings.getAvro().getProperties()));
    }

    private Map<String, Object> avroProperties() {
        Map<String, Object> avro = new HashMap<>();
        SchemaRegistrySettings.Avro settings = this.settings.getAvro();
        if (settings.getEncoding() != null) {
            avro.put(AvroSerdeConfig.AVRO_ENCODING, settings.getEncoding().name());
        }
        ApicurioConfiguration.putIfSet(avro, AvroSerdeConfig.AVRO_VALIDATE_WRITER_SCHEMA,
                settings.getValidateWriterSchema());
        if (settings.getDatumProvider() != null) {
            avro.put(AvroSerdeConfig.AVRO_DATUM_PROVIDER, settings.getDatumProvider().getClassName());
        }
        return avro;
    }
}
