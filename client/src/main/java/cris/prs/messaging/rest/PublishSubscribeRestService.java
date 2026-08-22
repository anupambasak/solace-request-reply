package cris.prs.messaging.rest;

import cris.prs.messaging.Notification;
import cris.prs.messaging.service.NotificationPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

/**
 * Publish-subscribe endpoints.
 *
 * <p>Every notification published here is delivered to <em>every</em> running server instance,
 * because each binds its own endpoint. Scale the server up and the same notification appears in
 * every pod's log.</p>
 *
 * <p>The three endpoints differ in how the messages are published, not in what they contain:
 * one message, several independent publishes, or several publishes in a single Solace
 * transaction.</p>
 */
@Slf4j
@RestController
@RequestMapping("/pub-sub")
@RequiredArgsConstructor
public class PublishSubscribeRestService {

    private final NotificationPublisher notificationPublisher;

    /**
     * Broadcast a single notification.
     *
     * @param message the text to broadcast
     * @return the notification that was published
     */
    @GetMapping("/publish")
    public Mono<Notification> publish(
            @RequestParam(defaultValue = "hello from the client") String message) {
        return Mono.fromCallable(() -> notificationPublisher.publish(message))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Broadcast several notifications as independent publishes.
     *
     * <p>Each reaches the broker as it is sent, so a failure part way through leaves the earlier
     * ones delivered.</p>
     *
     * @param message the text to broadcast, suffixed with each notification's position
     * @param count   how many to publish
     * @return the notifications that were published, streamed as they complete
     */
    @GetMapping("/publish-multiple")
    public Flux<Notification> publishMultiple(
            @RequestParam(defaultValue = "hello from the client") String message,
            @RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> notificationPublisher.publishMultiple(message, count))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(Flux::fromIterable);
    }

    /**
     * Broadcast several notifications in one Solace local transaction.
     *
     * <p>Nothing reaches the broker until the transaction commits, so subscribers see the whole
     * batch or none of it.</p>
     *
     * @param message the text to broadcast, suffixed with each notification's position
     * @param count   how many to publish in the transaction
     * @return the notifications that were published
     */
    @GetMapping("/publish-batch")
    public Mono<List<Notification>> publishBatch(
            @RequestParam(defaultValue = "hello from the client") String message,
            @RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> notificationPublisher.publishBatch(message, count))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
