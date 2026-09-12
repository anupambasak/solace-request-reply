package org.cris.prs.messaging.solace.schema;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.apicurio.registry.serde.protobuf.ProtobufDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufDeserializerConfig;
import io.apicurio.registry.serde.protobuf.ProtobufSerializer;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Google Protocol Buffers through Apicurio Registry.
 *
 * <p>Serialises any {@code com.google.protobuf.Message} &mdash; generated classes and
 * {@code DynamicMessage} alike; Apicurio writes the message type name ahead of the body so a consumer can
 * find it in a schema holding several.</p>
 *
 * <p>One deserializer serves every listener. It yields a {@code DynamicMessage} (or, with
 * {@code protobuf.derive-class}, the class named by the schema's Java options), and when the listener asks
 * for a specific generated type this codec re-parses the message into it with the type's static
 * {@code parseFrom(ByteString)}. Apicurio's own alternative &mdash; a specific return class &mdash; is one
 * class per deserializer, which cannot serve a container whose listeners want different types.</p>
 *
 * <p>Needs {@code io.apicurio:apicurio-registry-serde-common-protobuf} on the classpath.</p>
 */
public class ProtobufSchemaCodec extends ApicurioSchemaCodec {

    private final Lazy<ProtobufSerializer<Message>> serializer = new Lazy<>(this::createSerializer);

    private final Lazy<ProtobufDeserializer<Message>> deserializer = new Lazy<>(this::createDeserializer);

    private final Map<Class<?>, Method> parseMethods = new ConcurrentHashMap<>();

    /**
     * Create a codec. Nothing contacts the registry until the first message.
     *
     * @param settings validated settings
     */
    public ProtobufSchemaCodec(SchemaRegistrySettings settings) {
        super(settings);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaFormat getFormat() {
        return SchemaFormat.PROTOBUF;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSchemaPayload(Object payload) {
        return payload instanceof Message;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Always {@code true}: a generated Protobuf message carries its schema in its descriptor.</p>
     */
    @Override
    public boolean canDeriveSchema() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Reads the {@code .proto} source out of the generated message's file descriptor, so the whole file
     * &mdash; every message type it declares &mdash; is published as one artifact, exactly as Apicurio
     * registers it from the data. {@code payloadClass} must be a generated {@code com.google.protobuf.Message}
     * (or its file class), the only kind with a descriptor.</p>
     */
    @Override
    public String deriveSchema(Class<?> payloadClass) {
        Descriptors.FileDescriptor file = fileDescriptorOf(payloadClass);
        ProtoFileElement element = FileDescriptorUtils.fileDescriptorToProtoFile(file.toProto());
        return element.toSchema();
    }

    private Descriptors.FileDescriptor fileDescriptorOf(Class<?> payloadClass) {
        Object descriptor;
        try {
            Method getDescriptor = payloadClass.getMethod("getDescriptor");
            descriptor = getDescriptor.invoke(null);
        }
        catch (ReflectiveOperationException ex) {
            throw new IllegalStateException(payloadClass.getName() + " is not a generated Protobuf message class: "
                    + "it has no static getDescriptor()", ex);
        }
        if (descriptor instanceof Descriptors.Descriptor messageType) {
            return messageType.getFile();
        }
        if (descriptor instanceof Descriptors.FileDescriptor fileDescriptor) {
            return fileDescriptor;
        }
        throw new IllegalStateException(payloadClass.getName() + ".getDescriptor() returned "
                + (descriptor == null ? "null" : descriptor.getClass().getName())
                + ", which is neither a Protobuf message nor a file descriptor");
    }

    /** {@inheritDoc} */
    @Override
    public boolean producesType(Class<?> targetType) {
        return targetType != null && Message.class.isAssignableFrom(targetType);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] serialize(String destinationName, Object payload) {
        return this.serializer.get().serializeData(destinationName, (Message) payload);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Re-parses the decoded message into {@code targetType} when that is a generated message class the
     * decoded value is not already an instance of.</p>
     */
    @Override
    public Object deserialize(String destinationName, byte[] body, Class<?> targetType) {
        Message message = this.deserializer.get().deserializeData(destinationName, body);
        if (message == null || targetType == null || targetType.isInstance(message)
                || !Message.class.isAssignableFrom(targetType)) {
            return message;
        }
        return reparse(message.toByteString(), targetType);
    }

    private Object reparse(ByteString bytes, Class<?> targetType) {
        Method parseFrom = this.parseMethods.computeIfAbsent(targetType, type -> {
            try {
                return type.getMethod("parseFrom", ByteString.class);
            }
            catch (NoSuchMethodException ex) {
                throw new IllegalStateException(type.getName() + " is not a generated Protobuf message class: it has "
                        + "no static parseFrom(ByteString)", ex);
            }
        });
        try {
            return parseFrom.invoke(null, bytes);
        }
        catch (InvocationTargetException ex) {
            throw new IllegalStateException("Unable to parse the message as " + targetType.getName(), ex.getCause());
        }
        catch (IllegalAccessException ex) {
            throw new IllegalStateException("Unable to call " + targetType.getName() + ".parseFrom", ex);
        }
    }

    private ProtobufSerializer<Message> createSerializer() {
        Map<String, Object> protobuf = new HashMap<>();
        ApicurioConfiguration.putIfSet(protobuf, SerdeConfig.VALIDATION_ENABLED, this.settings.getProtobuf().getValidation());
        return configured(new ProtobufSerializer<>(), configuration(protobuf, this.settings.getProtobuf().getProperties()));
    }

    private ProtobufDeserializer<Message> createDeserializer() {
        Map<String, Object> protobuf = new HashMap<>();
        ApicurioConfiguration.putIfSet(protobuf, ProtobufDeserializerConfig.DERIVE_CLASS_FROM_SCHEMA,
                this.settings.getProtobuf().getDeriveClass());
        return configured(new ProtobufDeserializer<>(), configuration(protobuf, this.settings.getProtobuf().getProperties()));
    }
}
