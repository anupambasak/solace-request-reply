package cris.prs.messaging.consumer;

import cris.prs.messaging.Notification;
import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.SolaceRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Topic dispatch: three handler methods, one endpoint.
 *
 * <p>Each method declares the same {@code queue} and {@code group} plus
 * {@code topicDispatch = "true"}, so instead of three durable queues, three provisioning rounds and
 * three sets of binds, the container binds <b>one</b> endpoint carrying the union of the three
 * subscriptions and routes each message to the method whose subscription matched.</p>
 *
 * <p>The catch-all is declared <b>last</b> on purpose. The first matching target wins, in declaration
 * order, so a wildcard declared before the specific subscriptions would swallow everything.</p>
 */
@Slf4j
@Component
public class NotificationRouter {

    private final AtomicInteger deployments = new AtomicInteger();

    private final AtomicInteger incidents = new AtomicInteger();

    private final AtomicInteger other = new AtomicInteger();

    @SolaceListener(
            id = "notificationRouter",
            pattern = "POINT_TO_POINT",
            queue = "${app.router.queue:notification-router}",
            group = "${app.router.group:v1}",
            topics = {"${app.router.topic-prefix:routed}/deployment/>"},
            topicDispatch = "true")
    public void onDeployment(Notification notification) {
        log.info("Deployment notification {}: {}", this.deployments.incrementAndGet(),
                notification.getMessage());
    }

    @SolaceListener(
            pattern = "POINT_TO_POINT",
            queue = "${app.router.queue:notification-router}",
            group = "${app.router.group:v1}",
            topics = {"${app.router.topic-prefix:routed}/incident/*"},
            topicDispatch = "true")
    public void onIncident(Notification notification, SolaceRecord<Notification> record) {
        log.warn("Incident notification {} on {}: {}", this.incidents.incrementAndGet(),
                record.getDestination(), notification.getMessage());
    }

    /** Declared last: the first matching target wins, so a catch-all must come after the specifics. */
    @SolaceListener(
            pattern = "POINT_TO_POINT",
            queue = "${app.router.queue:notification-router}",
            group = "${app.router.group:v1}",
            topics = {"${app.router.topic-prefix:routed}/>"},
            topicDispatch = "true")
    public void onAnythingElse(SolaceRecord<Notification> record) {
        log.info("Unrouted notification {} on {}", this.other.incrementAndGet(),
                record.getDestination());
    }

    /**
     * How many deployment notifications this instance has handled.
     *
     * @return the count
     */
    public int getDeploymentCount() {
        return this.deployments.get();
    }

    /**
     * How many incident notifications this instance has handled.
     *
     * @return the count
     */
    public int getIncidentCount() {
        return this.incidents.get();
    }

    /**
     * How many notifications fell through to the catch-all.
     *
     * @return the count
     */
    public int getOtherCount() {
        return this.other.get();
    }
}
