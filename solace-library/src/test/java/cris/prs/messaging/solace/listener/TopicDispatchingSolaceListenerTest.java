package cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Routing behaviour for a shared endpoint: the right method is chosen, declaration order breaks ties,
 * and an unclaimed message is acknowledged rather than redelivered forever.
 */
class TopicDispatchingSolaceListenerTest {

    @Test
    @DisplayName("routes each message to the target whose subscription matched")
    void routesByTopic() throws Exception {
        AtomicReference<String> handled = new AtomicReference<>();
        TopicDispatchingSolaceListener dispatcher = new TopicDispatchingSolaceListener("orders",
                List.of(target("created", handled, "orders/created"),
                        target("cancelled", handled, "orders/cancelled")));

        dispatcher.onMessage(messageOn("orders/cancelled"));

        assertEquals("cancelled", handled.get());
    }

    @Test
    @DisplayName("honours wildcards in a target's subscriptions")
    void routesByWildcard() throws Exception {
        AtomicReference<String> handled = new AtomicReference<>();
        TopicDispatchingSolaceListener dispatcher = new TopicDispatchingSolaceListener("orders",
                List.of(target("audit", handled, "orders/*/audit")));

        dispatcher.onMessage(messageOn("orders/eu/audit"));

        assertEquals("audit", handled.get());
    }

    @Test
    @DisplayName("the first matching target wins, so overlaps resolve by declaration order")
    void firstMatchWins() throws Exception {
        AtomicReference<String> handled = new AtomicReference<>();
        TopicDispatchingSolaceListener dispatcher = new TopicDispatchingSolaceListener("orders",
                List.of(target("specific", handled, "orders/created"),
                        target("catchAll", handled, "orders/>")));

        dispatcher.onMessage(messageOn("orders/created"));

        assertEquals("specific", handled.get(),
                "declaring the specific subscription first is what makes it win");
    }

    @Test
    @DisplayName("an unclaimed message is acknowledged, not failed — a retry would never match either")
    void unmatchedIsAcknowledged() throws Exception {
        AtomicReference<String> handled = new AtomicReference<>();
        TopicDispatchingSolaceListener dispatcher = new TopicDispatchingSolaceListener("orders",
                List.of(target("created", handled, "orders/created")));

        dispatcher.onMessage(messageOn("payments/settled"));

        assertNull(handled.get());
    }

    private TopicDispatchingSolaceListener.Target target(String name, AtomicReference<String> handled,
            String... topics) {
        return new TopicDispatchingSolaceListener.Target(List.of(topics),
                message -> handled.set(name), name);
    }

    /** A message that only has to answer for its destination. */
    private BytesXMLMessage messageOn(String topic) {
        Destination destination = (Destination) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {Destination.class},
                (InvocationHandler) (proxy, method, args) ->
                        "getName".equals(method.getName()) ? topic : null);
        return (BytesXMLMessage) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {BytesXMLMessage.class},
                (InvocationHandler) (proxy, method, args) ->
                        "getDestination".equals(method.getName()) ? destination : null);
    }
}
