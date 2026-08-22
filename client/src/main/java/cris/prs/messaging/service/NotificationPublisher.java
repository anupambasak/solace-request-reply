package cris.prs.messaging.service;

import cris.prs.messaging.Notification;
import cris.prs.messaging.solace.core.SolaceTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Publish-subscribe producer.
 *
 * <p>Publishing is pattern agnostic: the producer sends to a topic and is finished. Whether that
 * becomes fan-out or competing consumers is decided entirely by how the consumers bind &mdash; see
 * {@code NotificationSubscriber} on the server.</p>
 */
@Slf4j
@Service
public class NotificationPublisher {

    private final SolaceTemplate<Object> solace;

    private final String topic;

    public NotificationPublisher(SolaceTemplate<Object> solace,
            @Value("${app.notification.topic:notification/broadcast}") String topic) {
        this.solace = solace;
        this.topic = topic;
    }

    /** Broadcast a notification; every subscribing instance receives its own copy. */
    public Notification publish(String message) {
        Notification notification =
                new Notification(UUID.randomUUID().toString(), message, System.currentTimeMillis());
        this.solace.send(this.topic, notification);
        log.debug("Published notification {} to {}", notification.getId(), this.topic);
        return notification;
    }
}
