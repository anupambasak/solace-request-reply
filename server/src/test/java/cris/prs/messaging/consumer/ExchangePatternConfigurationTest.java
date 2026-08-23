package cris.prs.messaging.consumer;

import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.core.ExchangePattern;
import cris.prs.messaging.solace.listener.ContainerProperties;
import cris.prs.messaging.solace.listener.SolaceListenerEndpoint;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The exchange pattern is declared on the annotation, so this is where the wiring it implies is
 * pinned down. These assertions are the difference between fan-out and competing consumers: get the
 * endpoint naming or access type wrong and the service silently either duplicates work or drops
 * two thirds of it.
 */
class ExchangePatternConfigurationTest {

    private static final String INSTANCE_ID = "server-abc123";

    private static SolaceListener annotationOn(Class<?> type, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        Method method = type.getMethod(methodName, parameterTypes);
        SolaceListener annotation = method.getAnnotation(SolaceListener.class);
        assertThat(annotation).as("@SolaceListener on %s.%s", type.getSimpleName(), methodName).isNotNull();
        return annotation;
    }

    @Nested
    @DisplayName("publish-subscribe")
    class PublishSubscribe {

        @Test
        @DisplayName("is declared on the notification subscriber")
        void isDeclaredOnTheSubscriber() throws NoSuchMethodException {
            SolaceListener annotation =
                    annotationOn(NotificationSubscriber.class, "onNotification", cris.prs.messaging.Notification.class);

            assertThat(annotation.pattern()).isEqualTo("PUBLISH_SUBSCRIBE");
            assertThat(annotation.topics()).containsExactly("${app.notification.topic:notification/broadcast}");
        }

        @Test
        @DisplayName("gives every instance its own non-durable endpoint, so all of them receive every message")
        void bindsAPerInstanceEndpoint() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setId("notificationSubscriber");
            endpoint.setPattern(ExchangePattern.PUBLISH_SUBSCRIBE);
            endpoint.setQueue("notification");
            endpoint.setTopics(List.of("notification/broadcast"));

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getEndpointMode()).isEqualTo(EndpointMode.NON_DURABLE_QUEUE);
            assertThat(endpoint.getAppendInstanceIdToQueue()).isTrue();
            assertThat(endpoint.getAccessType()).isEqualTo(ContainerProperties.AccessType.EXCLUSIVE);
            assertThat(endpoint.resolveQueueName(INSTANCE_ID)).isEqualTo("notification." + INSTANCE_ID);
        }

        @Test
        @DisplayName("binds a single flow, because an exclusive endpoint admits one consumer")
        void bindsASingleFlow() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setPattern(ExchangePattern.PUBLISH_SUBSCRIBE);

            endpoint.applyPatternDefaults();

            // Without this the container inherits solace.listener.concurrency (10 on the server) and
            // the broker rejects the surplus binds with 503 Max clients exceeded for queue.
            assertThat(endpoint.getConcurrency()).isEqualTo(1);
        }

        @Test
        @DisplayName("subscribes every instance to the same topic, not to per-instance topics")
        void keepsTheTopicShared() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setPattern(ExchangePattern.PUBLISH_SUBSCRIBE);
            endpoint.setTopics(List.of("notification/broadcast"));

            endpoint.applyPatternDefaults();

            assertThat(endpoint.resolveTopics(INSTANCE_ID)).containsExactly("notification/broadcast");
        }
    }

    @Nested
    @DisplayName("point-to-point")
    class PointToPoint {

        @Test
        @DisplayName("is declared on the task worker")
        void isDeclaredOnTheWorker() throws NoSuchMethodException {
            SolaceListener annotation =
                    annotationOn(TaskWorker.class, "onTask", cris.prs.messaging.Task.class);

            assertThat(annotation.pattern()).isEqualTo("POINT_TO_POINT");
            assertThat(annotation.queue()).isEqualTo("${app.task.queue:task}");
            assertThat(annotation.group()).isEqualTo("${app.task.group:workers}");
        }

        @Test
        @DisplayName("shares one durable non-exclusive endpoint, so each task is handled exactly once")
        void bindsASharedEndpoint() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setId("taskWorker");
            endpoint.setPattern(ExchangePattern.POINT_TO_POINT);
            endpoint.setQueue("task");
            endpoint.setGroup("workers");
            endpoint.setTopics(List.of("task/submit"));

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getEndpointMode()).isEqualTo(EndpointMode.DURABLE_QUEUE);
            assertThat(endpoint.getAppendInstanceIdToQueue()).isFalse();
            assertThat(endpoint.getAccessType()).isEqualTo(ContainerProperties.AccessType.NONEXCLUSIVE);
            // Identical on every instance: that shared name is what makes them compete.
            assertThat(endpoint.resolveQueueName(INSTANCE_ID)).isEqualTo("task.workers");
            assertThat(endpoint.resolveQueueName("a-different-host")).isEqualTo("task.workers");
        }

        @Test
        @DisplayName("leaves concurrency to configuration: a shared endpoint consumes in parallel")
        void leavesConcurrencyToConfiguration() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setPattern(ExchangePattern.POINT_TO_POINT);

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getConcurrency()).isNull();
        }
    }

    @Nested
    @DisplayName("request-reply")
    class RequestReply {

        @Test
        @DisplayName("shares the request endpoint the way point-to-point does")
        void sharesTheRequestEndpoint() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setPattern(ExchangePattern.REQUEST_REPLY);
            endpoint.setQueue("request-reply-queue-1");
            endpoint.setGroup("request-reply-group-1");

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getAppendInstanceIdToQueue()).isFalse();
            assertThat(endpoint.resolveQueueName(INSTANCE_ID)).isEqualTo("request-reply-queue-1.request-reply-group-1");
        }

        @Test
        @DisplayName("a second service has its own endpoint AND its own request topic")
        void secondServiceIsFullyIndependent() throws NoSuchMethodException {
            SolaceListener one = annotationOn(ServiceConsumer.class, "booking",
                    cris.prs.messaging.Person.class, String.class);
            SolaceListener two = annotationOn(QuoteConsumer.class, "quote",
                    cris.prs.messaging.Person.class, String.class);

            assertThat(two.pattern()).isEqualTo("REQUEST_REPLY");
            assertThat(two.queue()).isEqualTo("${app.quote.queue:request-reply-queue-2}");
            assertThat(two.group()).isEqualTo("${app.quote.group:request-reply-group-2}");

            // Separate endpoints keep the two services' backlogs apart...
            assertThat(two.queue()).isNotEqualTo(one.queue());
            // ...and separate request topics are what stop both of them answering the same request:
            // two queues subscribed to one topic would each receive a copy.
            assertThat(two.topics()).doesNotContainAnyElementsOf(java.util.List.of(one.topics()));
        }

        @Test
        @DisplayName("returns a value, which the container publishes to the requester's replyTo")
        void handlerReturnsTheReply() throws NoSuchMethodException {
            Method booking = ServiceConsumer.class.getMethod("booking",
                    cris.prs.messaging.Person.class, String.class);

            assertThat(booking.getReturnType()).isEqualTo(cris.prs.messaging.Person.class);
        }

        @Test
        @DisplayName("the reply type is per service, not fixed by the library")
        void replyTypeIsPerService() throws NoSuchMethodException {
            assertThat(QuoteConsumer.class
                    .getMethod("quote", cris.prs.messaging.Person.class, String.class)
                    .getReturnType())
                    .isEqualTo(cris.prs.messaging.Quote.class);
        }
    }

    @Nested
    @DisplayName("explicit attributes")
    class ExplicitAttributes {

        @Test
        @DisplayName("win over the pattern's defaults")
        void overrideThePatternDefaults() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setPattern(ExchangePattern.PUBLISH_SUBSCRIBE);
            endpoint.setQueue("notification");
            // A durable fan-out endpoint: every instance still gets its own, but it survives restarts.
            endpoint.setEndpointMode(EndpointMode.DURABLE_QUEUE);

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getEndpointMode()).isEqualTo(EndpointMode.DURABLE_QUEUE);
            assertThat(endpoint.getAppendInstanceIdToQueue()).isTrue();
        }

        @Test
        @DisplayName("leave an endpoint with no pattern entirely to the container defaults")
        void areUntouchedWithoutAPattern() {
            SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
            endpoint.setQueue("request-reply-queue-1");

            endpoint.applyPatternDefaults();

            assertThat(endpoint.getEndpointMode()).isNull();
            assertThat(endpoint.getAccessType()).isNull();
            assertThat(endpoint.getAppendInstanceIdToQueue()).isNull();
        }
    }
}
