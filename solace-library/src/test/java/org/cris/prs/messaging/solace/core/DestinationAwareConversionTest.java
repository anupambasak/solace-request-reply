package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The two core changes Schema Registry support rests on: a converter can see where a message is going,
 * and a header never overwrites a user property the converter wrote.
 */
class DestinationAwareConversionTest {

    /** Records the destination it was given, and writes a user property as the schema registry converter does. */
    static class RecordingConverter implements SolaceMessageConverter {

        final List<String> destinations = new ArrayList<>();

        @Override
        public XMLMessage toMessage(Object payload) {
            return toMessage(payload, null);
        }

        @Override
        public XMLMessage toMessage(Object payload, String destination) {
            this.destinations.add(destination);
            try {
                BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                message.setData(new byte[] {1});
                SDTMap properties = JCSMPFactory.onlyInstance().createMap();
                properties.putString("schemaId", "from-converter");
                message.setProperties(properties);
                return message;
            }
            catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }

        @Override
        public Object fromMessage(BytesXMLMessage message, Class<?> targetType) {
            return message;
        }
    }

    /** A template that captures instead of publishing, so no session is needed. */
    static class CapturingTemplate extends SolaceTemplate<Object> {

        final List<XMLMessage> sent = new ArrayList<>();

        CapturingTemplate(SolaceMessageConverter converter) {
            super((SolaceSessionFactory) Proxy.newProxyInstance(SolaceSessionFactory.class.getClassLoader(),
                    new Class<?>[] {SolaceSessionFactory.class}, (proxy, method, args) -> null), converter);
        }

        @Override
        public void send(Destination destination, XMLMessage message) {
            this.sent.add(message);
        }
    }

    @Test
    @DisplayName("every send path hands the converter its destination")
    void sendPassesDestination() {
        RecordingConverter converter = new RecordingConverter();
        CapturingTemplate template = new CapturingTemplate(converter);

        template.send("orders/place", "payload");
        template.send("queue:orders-q", 42, Map.of("tenant", "eu"));
        template.send(MessageBuilder.withPayload("payload")
                .setHeader(SolaceHeaders.TARGET_DESTINATION, "orders/cancel").build());

        assertEquals(List.of("orders/place", "queue:orders-q", "orders/cancel"), converter.destinations);
    }

    @Test
    @DisplayName("the old createMessage overload still works, with no destination")
    void legacyCreateMessage() {
        RecordingConverter converter = new RecordingConverter();

        new CapturingTemplate(converter).createMessage("payload", null);

        assertEquals(1, converter.destinations.size());
        assertNull(converter.destinations.get(0));
    }

    @Test
    @DisplayName("a header never overwrites a user property the converter wrote")
    void converterOwnsItsProperties() throws Exception {
        CapturingTemplate template = new CapturingTemplate(new RecordingConverter());

        XMLMessage message = template.createMessage("app/reply/pod-1", "payload",
                Map.of("schemaId", "stale-from-request", "tenant", "eu"));

        assertEquals("from-converter", message.getProperties().getString("schemaId"));
        assertEquals("eu", message.getProperties().getString("tenant"));
    }

    @Test
    @DisplayName("a converter that does not override the new method is called through the old one")
    void defaultMethodDelegates() {
        SolaceMessageConverter legacy = new JacksonSolaceMessageConverter();

        XMLMessage message = legacy.toMessage("text", "orders/place");

        assertInstanceOf(BytesMessage.class, message);
    }
}
