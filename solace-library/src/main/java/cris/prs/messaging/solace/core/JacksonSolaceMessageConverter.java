package cris.prs.messaging.solace.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;

import java.nio.charset.StandardCharsets;

/**
 * Default {@link SolaceMessageConverter}: JSON in the binary attachment of a {@code BytesMessage}.
 *
 * <p>{@code byte[]} and {@code String} payloads are passed through untouched so that the converter
 * can also be used for opaque or text protocols.</p>
 */
public class JacksonSolaceMessageConverter implements SolaceMessageConverter {

    private final ObjectMapper objectMapper;

    public JacksonSolaceMessageConverter() {
        this(new ObjectMapper());
    }

    public JacksonSolaceMessageConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
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
        try {
            return this.objectMapper.readValue(body, targetType);
        }
        catch (Exception ex) {
            throw new SolaceMessagingException("Unable to deserialize message body into "
                    + targetType.getName(), ex);
        }
    }

    private byte[] extractBody(BytesXMLMessage message) {
        if (message instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            return text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        }
        byte[] bytes = message.getBytes();
        return bytes == null ? new byte[0] : bytes;
    }
}
