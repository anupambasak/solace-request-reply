package org.cris.prs.messaging.solace.schema;

/**
 * One schema format's serializer and deserializer: the seam between
 * {@link SchemaRegistrySolaceMessageConverter} and Apicurio.
 *
 * <p>The converter decides <em>whether</em> a message goes through the registry, and <em>which</em>
 * format, from the payload, the destination and the message itself; a codec does the serde work, in bytes.
 * Keeping Apicurio types behind this interface is what keeps the Apicurio jars optional, and lets the
 * converter's rules be tested with a fake codec and no registry.</p>
 *
 * <p>Implementations must be thread safe, and {@link #close()} must be idempotent.</p>
 *
 * @see AvroSchemaCodec
 * @see ProtobufSchemaCodec
 * @see JsonSchemaCodec
 */
public interface SchemaCodec extends AutoCloseable {

    /**
     * The schema language this codec speaks.
     *
     * @return the format; never {@code null}
     */
    SchemaFormat getFormat();

    /**
     * Whether a payload is natively a value of this format &mdash; an Avro record, a Protobuf message, a
     * {@code JsonNode}. Such a payload goes through the registry whatever its destination.
     *
     * @param payload the outbound payload, never {@code null}
     * @return {@code true} if this codec can serialise the payload as it is
     */
    boolean isSchemaPayload(Object payload);

    /**
     * Whether this codec can also serialise a plain Java object &mdash; one for which
     * {@link #isSchemaPayload(Object)} is {@code false} &mdash; sent to a governed destination.
     *
     * @return {@code true} for JSON Schema, and for Avro with a reflect datum provider; the default is
     *         {@code false}
     */
    default boolean acceptsPojos() {
        return false;
    }

    /**
     * Whether this format can produce the given listener type, used to pick a codec for a registry-framed
     * message that does not say which format it is.
     *
     * @param targetType the type a listener asked for
     * @return {@code true} if the type is specific to this format (an Avro record type, a Protobuf message
     *         type). A format that can produce anything, like JSON Schema, returns {@code false}; the
     *         converter treats it as the default instead
     */
    boolean producesType(Class<?> targetType);

    /**
     * Serialise a payload into a registry-framed body.
     *
     * @param destinationName the topic or queue the message is for, used by the artifact resolver
     *                        strategy; may be {@code null}
     * @param payload         the payload
     * @return the framed body: magic byte, schema id, encoded payload
     */
    byte[] serialize(String destinationName, Object payload);

    /**
     * Deserialise a registry-framed body.
     *
     * @param destinationName where the message was received; may be {@code null}
     * @param body            a body for which {@link SchemaRegistryHeaders#isFramed(byte[])} is true
     * @param targetType      the type the listener asked for, which may select how the codec decodes. The
     *                        result need not be an instance of it: a JSON Schema codec returns a
     *                        {@code JsonNode}, which the converter maps onto the target type
     * @return the decoded value
     */
    Object deserialize(String destinationName, byte[] body, Class<?> targetType);

    /**
     * Whether this codec can derive a schema from a payload class, without a message or a registry call
     * &mdash; so its schema can be published at application initialization.
     *
     * <p>Avro derives from a class (a generated {@code SpecificRecord}, or any class with a reflect datum
     * provider); Protobuf reads the schema out of a generated message's descriptor. JSON Schema cannot be
     * inferred from a class, so it keeps the default {@code false}: those schemas are declared as files
     * under {@code registration.schemas}.</p>
     *
     * @return {@code true} if {@link #deriveSchema(Class)} is supported; the default is {@code false}
     */
    default boolean canDeriveSchema() {
        return false;
    }

    /**
     * Derive this format's schema text from a payload class, for publishing to the registry at
     * application initialization. Touches neither a message nor the registry.
     *
     * @param payloadClass the class whose schema to derive; never {@code null}
     * @return the schema content, in this format's language (an Avro {@code .avsc} JSON document, a
     *         Protobuf {@code .proto} source file)
     * @throws UnsupportedOperationException if this format cannot derive a schema from a class
     *         ({@link #canDeriveSchema()} is {@code false})
     * @throws IllegalStateException if the class is not one this format can derive a schema from
     */
    default String deriveSchema(Class<?> payloadClass) {
        throw new UnsupportedOperationException(
                getFormat() + " cannot derive a schema from a class; declare it under "
                        + "solace.schema-registry.registration.schemas instead");
    }

    /** Release the registry client. Idempotent; the default does nothing. */
    @Override
    default void close() {
    }
}
