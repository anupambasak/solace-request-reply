package org.cris.prs.messaging.solace.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * A codec that frames bodies the way Apicurio does &mdash; magic byte, four-byte id, payload &mdash; with
 * the payload as plain JSON, so the converter's routing can be tested without a registry.
 */
class FakeSchemaCodec implements SchemaCodec {

    private final ObjectMapper mapper = new ObjectMapper();

    private final SchemaFormat format;

    private final Predicate<Object> nativePayload;

    private final Class<?> nativeType;

    final List<String> serializedTo = new ArrayList<>();

    final List<Object> serializedValues = new ArrayList<>();

    int deserializeCalls;

    RuntimeException failWith;

    /**
     * @param format        the format to claim
     * @param nativeType    the payload type this format recognises natively, or {@code null} for none
     */
    FakeSchemaCodec(SchemaFormat format, Class<?> nativeType) {
        this.format = format;
        this.nativeType = nativeType;
        this.nativePayload = payload -> nativeType != null && nativeType.isInstance(payload);
    }

    @Override
    public SchemaFormat getFormat() {
        return this.format;
    }

    @Override
    public boolean isSchemaPayload(Object payload) {
        return this.nativePayload.test(payload);
    }

    @Override
    public boolean producesType(Class<?> targetType) {
        return this.format != SchemaFormat.JSON_SCHEMA && this.nativeType != null && targetType != null
                && this.nativeType.isAssignableFrom(targetType);
    }

    @Override
    public byte[] serialize(String destinationName, Object payload) {
        if (this.failWith != null) {
            throw this.failWith;
        }
        this.serializedTo.add(destinationName);
        this.serializedValues.add(payload);
        try {
            return frame(this.mapper.writeValueAsBytes(payload));
        }
        catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public Object deserialize(String destinationName, byte[] body, Class<?> targetType) {
        this.deserializeCalls++;
        if (this.failWith != null) {
            throw this.failWith;
        }
        try {
            return this.mapper.readTree(Arrays.copyOfRange(body, 5, body.length));
        }
        catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** Apicurio framing around a body: magic byte, then a four-byte id of 42. */
    static byte[] frame(byte[] payload) {
        byte[] framed = new byte[payload.length + 5];
        framed[0] = SchemaRegistryHeaders.MAGIC_BYTE;
        framed[4] = 42;
        System.arraycopy(payload, 0, framed, 5, payload.length);
        return framed;
    }

    /**
     * A message as received on {@code topic}, optionally declaring its format.
     *
     * <p>JCSMP sets the destination of a received message itself and offers no setter, so the message is
     * wrapped in a proxy that reports the topic and delegates everything else.</p>
     */
    static BytesXMLMessage received(String topic, byte[] body, String schemaFormat) {
        try {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(body);
            if (schemaFormat != null) {
                SDTMap properties = JCSMPFactory.onlyInstance().createMap();
                properties.putString(SchemaRegistryHeaders.SCHEMA_FORMAT, schemaFormat);
                message.setProperties(properties);
            }
            Destination destination = JCSMPFactory.onlyInstance().createTopic(topic);
            return (BytesXMLMessage) Proxy.newProxyInstance(FakeSchemaCodec.class.getClassLoader(),
                    new Class<?>[] {BytesXMLMessage.class}, (proxy, method, args) -> {
                        if ("getDestination".equals(method.getName()) && method.getParameterCount() == 0) {
                            return destination;
                        }
                        try {
                            return method.invoke(message, args);
                        }
                        catch (InvocationTargetException ex) {
                            throw ex.getCause();
                        }
                    });
        }
        catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
