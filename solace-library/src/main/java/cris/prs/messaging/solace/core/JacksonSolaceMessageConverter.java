package cris.prs.messaging.solace.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Default {@link SolaceMessageConverter}: JSON in the binary attachment of a {@code BytesMessage}.
 *
 * <p>{@code byte[]} and {@code String} payloads are passed through untouched so that the converter
 * can also be used for opaque or text protocols.</p>
 */
public class JacksonSolaceMessageConverter implements SolaceMessageConverter {

    private final ObjectMapper objectMapper;

    /** Create a converter with its own {@code ObjectMapper} using Jackson's defaults. */
    public JacksonSolaceMessageConverter() {
        this(new ObjectMapper());
    }

    /**
     * Create a converter sharing an existing mapper, so that modules, naming strategies and
     * date handling match the rest of the application.
     *
     * @param objectMapper the mapper to serialise and deserialise payloads with
     */
    public JacksonSolaceMessageConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>Produces a {@code BytesMessage} whose <em>binary attachment</em> holds the body.
     * {@code byte[]} and {@code String} payloads are written through untouched; everything else is
     * serialised as JSON.</p>
     */
    public XMLMessage toMessage(Object payload) {
        byte[] body;
        if (payload == null) {
            body = new byte[0];
        }
        else if (payload instanceof byte[] bytes) {
            body = bytes;
        }
        else if (payload instanceof String text) {
            body = text.getBytes(StandardCharsets.UTF_8);
        }
        else {
            try {
                body = this.objectMapper.writeValueAsBytes(payload);
            }
            catch (Exception ex) {
                throw new SolaceMessagingException("Unable to serialize payload of type "
                        + payload.getClass().getName(), ex);
            }
        }
        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        message.setData(body);
        return message;
    }

    @Override
    /**
     * {@inheritDoc}
     *
     * <p>A {@code null}, {@code Object} or {@code BytesXMLMessage} target type returns the raw
     * message unconverted; {@code byte[]} and {@code String} return the body as-is.</p>
     */
    public Object fromMessage(BytesXMLMessage message, Class<?> targetType) {
        byte[] body = extractBody(message);
        if (targetType == null || Object.class.equals(targetType) || BytesXMLMessage.class.isAssignableFrom(targetType)) {
            return message;
        }
        if (byte[].class.equals(targetType)) {
            return body;
        }
        if (String.class.equals(targetType)) {
            return new String(body, StandardCharsets.UTF_8);
        }
        if (body.length == 0) {
            throw new SolaceMessagingException("Received a message with an empty body (no binary "
                    + "attachment and no XML content); cannot convert it to " + targetType.getName());
        }
        try {
            return this.objectMapper.readValue(body, targetType);
        }
        catch (Exception ex) {
            throw new SolaceMessagingException("Unable to deserialize message body into "
                    + targetType.getName(), ex);
        }
    }

    /**
     * Read the message body.
     *
     * <p>{@link #toMessage} writes the payload with {@code BytesMessage.setData}, which fills the
     * <em>binary attachment</em>. {@code BytesXMLMessage.getBytes()} reads the <em>XML content</em>
     * part instead &mdash; a different section of the message that stays empty here &mdash; so the
     * attachment is read first, with the XML content kept as a fallback for senders that use it.</p>
     */
    private byte[] extractBody(BytesXMLMessage message) {
        if (message instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            return text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        }
        ByteBuffer attachment = message.getAttachmentByteBuffer();
        if (attachment != null && attachment.hasRemaining()) {
            byte[] bytes = new byte[attachment.remaining()];
            attachment.get(bytes);
            return bytes;
        }
        byte[] xmlContent = message.getBytes();
        return xmlContent == null ? new byte[0] : xmlContent;
    }
}
