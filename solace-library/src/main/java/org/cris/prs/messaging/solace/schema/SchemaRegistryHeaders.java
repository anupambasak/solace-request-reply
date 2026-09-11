package org.cris.prs.messaging.solace.schema;

/**
 * User properties written by {@link SchemaRegistrySolaceMessageConverter}.
 *
 * <p>The schema id itself travels <em>in the body</em>, in Apicurio's standard framing, not in a
 * property. The format is written as an SDT user property as well, for two reasons: a consumer that
 * accepts several formats can pick the right deserializer without guessing, and a broker-side selector
 * can filter on it ({@code schemaFormat = 'AVRO'}).</p>
 */
public final class SchemaRegistryHeaders {

    private SchemaRegistryHeaders() {
    }

    /** The Apicurio artifact type of the body: {@code AVRO}, {@code PROTOBUF} or {@code JSON}. */
    public static final String SCHEMA_FORMAT = "schemaFormat";

    /** Apicurio's magic byte, the first byte of every registry-framed body. */
    public static final byte MAGIC_BYTE = 0x0;

    /**
     * Whether a body carries Apicurio's framing: the magic byte followed by at least a four-byte id.
     *
     * <p>Unambiguous against the plain JSON the fallback converter writes, which never starts with a
     * {@code NUL} byte.</p>
     *
     * @param body a message body; may be {@code null}
     * @return {@code true} if the body starts with {@link #MAGIC_BYTE} and is long enough to hold an id
     */
    public static boolean isFramed(byte[] body) {
        return body != null && body.length >= 5 && body[0] == MAGIC_BYTE;
    }
}
