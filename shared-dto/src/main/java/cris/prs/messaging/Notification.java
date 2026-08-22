package cris.prs.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Broadcast payload for the publish-subscribe demo: every subscribing instance receives its own
 * copy of each notification.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    private String id;

    private String message;

    /** Millisecond epoch at which the notification was published. */
    private long publishedAt;
}
