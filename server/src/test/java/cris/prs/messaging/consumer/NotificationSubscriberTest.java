package cris.prs.messaging.consumer;

import cris.prs.messaging.Notification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Publish-subscribe consumer: every delivered copy is processed. */
class NotificationSubscriberTest {

    @Test
    @DisplayName("records each notification it receives")
    void recordsEachNotification() {
        NotificationSubscriber subscriber = new NotificationSubscriber();

        subscriber.onNotification(new Notification("n-1", "first", 1L));
        subscriber.onNotification(new Notification("n-2", "second", 2L));

        assertThat(subscriber.getReceivedCount()).isEqualTo(2);
        assertThat(subscriber.getLastNotification().get().getId()).isEqualTo("n-2");
    }

    @Test
    @DisplayName("returns void: a broadcast has no requester to reply to")
    void handlerReturnsVoid() throws NoSuchMethodException {
        assertThat(NotificationSubscriber.class
                .getMethod("onNotification", Notification.class).getReturnType())
                .isEqualTo(void.class);
    }
}
