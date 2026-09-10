package cris.prs.messaging.service;

import cris.prs.messaging.Notification;
import org.cris.prs.messaging.solace.core.SolaceTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Publish-subscribe producer.
 *
 * <p>Fan-out is a property of how consumers bind, not of the send, so what matters here is that the
 * publisher addresses a <em>topic</em>, stamps each notification with its own identity, and emits
 * exactly one message per notification in every mode.</p>
 */
@ExtendWith(MockitoExtension.class)
class NotificationPublisherTest {

    private static final String TOPIC = "notification/broadcast";

    @Mock
    private SolaceTemplate<Object> solaceTemplate;

    private NotificationPublisher publisher() {
        return new NotificationPublisher(this.solaceTemplate, TOPIC);
    }

    @Nested
    @DisplayName("single")
    class Single {

        @Test
        @DisplayName("publishes the notification to the configured broadcast topic")
        void publishesToTheBroadcastTopic() {
            Notification returned = publisher().publish("deployment finished");

            ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
            verify(solaceTemplate).send(eq(TOPIC), payload.capture());

            assertThat(payload.getValue()).isInstanceOf(Notification.class);
            Notification published = (Notification) payload.getValue();
            assertThat(published.getMessage()).isEqualTo("deployment finished");
            assertThat(published.getId()).isNotBlank();
            assertThat(published.getPublishedAt()).isPositive();
            assertThat(returned).isEqualTo(published);
        }

        @Test
        @DisplayName("gives every notification a distinct id so subscribers can de-duplicate")
        void assignsADistinctIdPerNotification() {
            NotificationPublisher publisher = publisher();

            assertThat(publisher.publish("first").getId())
                    .isNotEqualTo(publisher.publish("second").getId());
        }
    }

    @Nested
    @DisplayName("multiple")
    class Multiple {

        @Test
        @DisplayName("publishes one message per notification")
        void publishesOnePerNotification() {
            List<Notification> published = publisher().publishMultiple("rollout", 3);

            assertThat(published).hasSize(3);
            verify(solaceTemplate, times(3)).send(eq(TOPIC), any(Notification.class));
        }

        @Test
        @DisplayName("numbers each notification so a partial delivery is recognisable")
        void numbersEachNotification() {
            assertThat(publisher().publishMultiple("rollout", 3))
                    .extracting(Notification::getMessage)
                    .containsExactly("rollout (1 of 3)", "rollout (2 of 3)", "rollout (3 of 3)");
        }
    }

    @Nested
    @DisplayName("batch")
    class Batch {

        @Test
        @DisplayName("publishes one message per notification, for the transaction to release together")
        void publishesOnePerNotification() {
            List<Notification> published = publisher().publishBatch("rollout", 4);

            assertThat(published).hasSize(4);
            verify(solaceTemplate, times(4)).send(eq(TOPIC), any(Notification.class));
        }
    }
}
