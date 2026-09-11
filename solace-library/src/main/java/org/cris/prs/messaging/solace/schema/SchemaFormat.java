package org.cris.prs.messaging.solace.schema;

/**
 * The schema languages supported through Apicurio Registry.
 *
 * <p>Each needs its own Apicurio serde module on the classpath; the library depends on none of them at
 * runtime, so an application adds the ones it uses.</p>
 */
public enum SchemaFormat {

    /**
     * Apache Avro. Payloads are Avro records &mdash; {@code GenericRecord} or a generated
     * {@code SpecificRecord}. Needs {@code io.apicurio:apicurio-registry-serde-common-avro}.
     */
    AVRO("AVRO", "io.apicurio.registry.serde.avro.AvroSerializer",
            "io.apicurio:apicurio-registry-serde-common-avro"),

    /**
     * Google Protocol Buffers. Payloads are generated {@code com.google.protobuf.Message} classes, or a
     * {@code DynamicMessage}. Needs {@code io.apicurio:apicurio-registry-serde-common-protobuf}.
     */
    PROTOBUF("PROTOBUF", "io.apicurio.registry.serde.protobuf.ProtobufSerializer",
            "io.apicurio:apicurio-registry-serde-common-protobuf"),

    /**
     * JSON Schema. Payloads are ordinary POJOs or Jackson {@code JsonNode}s, validated against the
     * schema. Needs {@code io.apicurio:apicurio-registry-serde-common-jsonschema}.
     */
    JSON_SCHEMA("JSON", "io.apicurio.registry.serde.jsonschema.JsonSchemaSerializer",
            "io.apicurio:apicurio-registry-serde-common-jsonschema");

    private final String artifactType;

    private final String serializerClassName;

    private final String artifact;

    SchemaFormat(String artifactType, String serializerClassName, String artifact) {
        this.artifactType = artifactType;
        this.serializerClassName = serializerClassName;
        this.artifact = artifact;
    }

    /**
     * The Apicurio artifact type for this format, which is also the value written to the
     * {@value SchemaRegistryHeaders#SCHEMA_FORMAT} user property.
     *
     * @return {@code AVRO}, {@code PROTOBUF} or {@code JSON}
     */
    public String getArtifactType() {
        return this.artifactType;
    }

    /**
     * A class whose presence proves this format's Apicurio serde module is on the classpath.
     *
     * @return the fully qualified name of the format's serializer
     */
    public String getSerializerClassName() {
        return this.serializerClassName;
    }

    /**
     * The Maven coordinates of the Apicurio module this format needs, for error messages.
     *
     * @return {@code group:artifact}
     */
    public String getArtifact() {
        return this.artifact;
    }

    /**
     * Resolve an artifact type, as written to the {@value SchemaRegistryHeaders#SCHEMA_FORMAT} user
     * property, back to a format.
     *
     * @param artifactType {@code AVRO}, {@code PROTOBUF} or {@code JSON}; case-insensitive
     * @return the format, or {@code null} if the value names none
     */
    public static SchemaFormat fromArtifactType(String artifactType) {
        if (artifactType == null) {
            return null;
        }
        for (SchemaFormat format : values()) {
            if (format.artifactType.equalsIgnoreCase(artifactType) || format.name().equalsIgnoreCase(artifactType)) {
                return format;
            }
        }
        return null;
    }
}
