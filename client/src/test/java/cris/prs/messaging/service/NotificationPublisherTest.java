package cris.prs.messaging.service;

import cris.prs.messaging.Notification;
import cris.prs.messaging.solace.core.SolaceTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/**
 * Publish-subscribe producer.
 *
 * <p>Fan-out is a property of how consumers bind, not of the send, so what matters here is that the
 * publisher addresses a <em>topic</em> and stamps each notification with its own identity.</p>
 */
@ExtendWith(MockitoExtension.class)
class NotificationPublisherTest {

    private static final String TOPIC = "notification/broadcast";

    @Mock
    private SolaceTemplate<Object> solaceTemplate;

    @Test
    @DisplayName("publishes the notification to the configured broadcast topic")
    void publishesToTheBroadcastTopic() {
        NotificationPublisher publisher = new NotificationPublisher(this.solaceTemplate, TOPIC);

        Notification returned = publisher.publish("deployment finished");

        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(this.solaceTemplate).send(org.mockito.ArgumentMatchers.eq(TOPIC), payload.capture());

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
        NotificationPublisher publisher = new NotificationPublisher(this.solaceTemplate, TOPIC);

        assertThat(publisher.publish("first").getId())
                .isNotEqualTo(publisher.publish("second").getId());
    }
}
