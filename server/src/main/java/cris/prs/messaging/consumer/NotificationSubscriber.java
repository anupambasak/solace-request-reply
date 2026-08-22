package cris.prs.messaging.consumer;

import cris.prs.messaging.Notification;
import cris.prs.messaging.solace.annotation.SolaceListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publish-subscribe consumer.
 *
 * <p>{@code PUBLISH_SUBSCRIBE} gives this listener its own non-durable endpoint named with the
 * instance id, so <em>every</em> running server receives <em>every</em> notification. Scaling the
 * deployment out multiplies the processing rather than sharing it &mdash; the opposite of
 * {@link TaskWorker}.</p>
 *
 * <p>The method returns {@code void}: a broadcast has no requester to reply to.</p>
 */
@Slf4j
@Component
public class NotificationSubscriber {

    private final AtomicInteger receivedCount = new AtomicInteger();

    @Getter
    private final AtomicReference<Notification> lastNotification = new AtomicReference<>();

    @SolaceListener(
            id = "notificationSubscriber",
            pattern = "PUBLISH_SUBSCRIBE",
            queue = "${app.notification.queue:notification}",
            topics = {"${app.notification.topic:notification/broadcast}"},
            transactional = "false")
    public void onNotification(Notification notification) {
        int count = this.receivedCount.incrementAndGet();
        this.lastNotification.set(notification);
        log.info("Notification {} received (total {}): {}", notification.getId(), count,
                notification.getMessage());
    }

    public int getReceivedCount() {
        return this.receivedCount.get();
    }
}
