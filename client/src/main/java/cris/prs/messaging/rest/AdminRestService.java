package cris.prs.messaging.rest;

import cris.prs.messaging.Task;
import org.cris.prs.messaging.solace.core.BrowseSpec;
import org.cris.prs.messaging.solace.core.SolaceBrowser;
import org.cris.prs.messaging.solace.core.SolaceRecord;
import org.cris.prs.messaging.solace.core.ReplayStartPoint;
import org.cris.prs.messaging.solace.core.SolaceTemplate;
import org.cris.prs.messaging.solace.listener.DefaultSolaceMessageListenerContainer;
import org.cris.prs.messaging.solace.listener.SolaceListenerEndpointRegistry;
import org.cris.prs.messaging.solace.listener.SolaceMessageListenerContainer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator endpoints: look inside a queue without consuming it.
 *
 * <p>Browsing is the answer to "what is actually stuck on that queue" &mdash; questions the metrics
 * can count but not show you. It reads spooled messages <b>without acknowledging them</b>, so nothing
 * is taken away from the consumer that should process them.</p>
 *
 * <p>Browsing blocks on the network, so every endpoint here runs on {@code boundedElastic} rather
 * than the event loop.</p>
 */
@Slf4j
@RestController
@RequestMapping("/admin")
public class AdminRestService {

    private final SolaceTemplate<Object> solace;

    private final SolaceListenerEndpointRegistry registry;

    /**
     * Create the controller.
     *
     * @param solace   the plain template; qualified explicitly because a second, request-reply
     *                 {@code SolaceTemplate} exists in this application
     * @param registry supplies the listener containers a replay is started on
     */
    public AdminRestService(@Qualifier("solaceTemplate") SolaceTemplate<Object> solace,
            SolaceListenerEndpointRegistry registry) {
        this.solace = solace;
        this.registry = registry;
    }

    /**
     * Re-deliver messages the broker still holds to one listener container.
     *
     * <p>Replay is an operational act, which is why it is an endpoint rather than configuration:
     * leaving a start point in an annotation would replay again on every restart.</p>
     *
     * <p><b>This affects the whole endpoint.</b> On a queue shared by several instances every
     * consumer of it receives the replayed messages, and handlers see them again &mdash; so anything
     * with side effects has to be idempotent. A replay the broker cannot satisfy (replay not enabled
     * for the VPN, or a log that no longer covers the period) fails the flow and arrives as a
     * {@code DOWN} flow event, not as an error from this call.</p>
     *
     * @param listenerId which container to replay, as given by {@code @SolaceListener(id = ...)}
     * @param from       {@code BEGINNING}, or an ISO-8601 instant such as
     *                   {@code 2026-08-23T10:15:30Z}
     * @return what was requested
     */
    @GetMapping("/replay")
    public Mono<Map<String, Object>> replay(@RequestParam String listenerId,
            @RequestParam(defaultValue = "BEGINNING") String from) {
        return Mono.fromCallable(() -> {
            SolaceMessageListenerContainer container = this.registry.getListenerContainer(listenerId);
            if (!(container instanceof DefaultSolaceMessageListenerContainer replayable)) {
                throw new IllegalArgumentException("No replayable Solace listener container named '"
                        + listenerId + "'. Known containers: " + this.registry.getListenerContainerIds());
            }
            ReplayStartPoint startPoint = ReplayStartPoint.parse(from);
            replayable.replay(startPoint);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("listenerId", listenerId);
            result.put("replayFrom", String.valueOf(startPoint));
            result.put("running", replayable.isRunning());
            return result;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * List the listener containers, with the state flow events give them.
     *
     * @return each container's id, running state, endpoint and replay setting
     */
    @GetMapping("/containers")
    public Mono<List<Map<String, Object>>> containers() {
        return Mono.fromCallable(() -> this.registry.getListenerContainers().stream()
                .map(AdminRestService::describeContainer)
                .toList());
    }

    /**
     * Summarise one container.
     *
     * @param container the container to describe
     * @return the fields an operator wants to see
     */
    private static Map<String, Object> describeContainer(SolaceMessageListenerContainer container) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("id", container.getListenerId());
        summary.put("running", container.isRunning());
        if (container instanceof DefaultSolaceMessageListenerContainer defaultContainer) {
            summary.put("endpoint", defaultContainer.getResolvedQueueName());
            summary.put("flows", defaultContainer.getActiveFlowCount());
            summary.put("active", defaultContainer.isActive());
            summary.put("degraded", defaultContainer.isDegraded());
            summary.put("lastFlowEvent", String.valueOf(defaultContainer.getLastFlowEvent()));
            summary.put("replayFrom", String.valueOf(defaultContainer.getReplayFrom()));
        }
        return summary;
    }

    /**
     * Count what is spooled on a queue.
     *
     * <p>Walks the queue rather than asking the broker for a depth &mdash; the client API has no
     * depth call, that is a SEMP question &mdash; so {@code limit} bounds the work on a large
     * backlog.</p>
     *
     * @param queue the queue to count
     * @param limit stop counting here
     * @return the number of messages seen, and whether the limit was reached
     */
    @GetMapping("/queue/depth")
    public Mono<Map<String, Object>> depth(@RequestParam(defaultValue = "task.workers") String queue,
            @RequestParam(defaultValue = "1000") int limit) {
        return Mono.fromCallable(() -> {
            try (SolaceBrowser<Object> browser = this.solace.browse(queue, Object.class)) {
                long counted = browser.stream(limit).count();
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("queue", queue);
                result.put("counted", counted);
                result.put("reachedLimit", counted == limit);
                return result;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Peek at the first few messages on a queue.
     *
     * @param queue the queue to browse
     * @param limit how many messages to return
     * @return a summary of each message, including how many times it has been delivered
     */
    @GetMapping("/queue/peek")
    public Mono<List<Map<String, Object>>> peek(
            @RequestParam(defaultValue = "task.workers") String queue,
            @RequestParam(defaultValue = "10") int limit) {
        return Mono.fromCallable(() -> {
            try (SolaceBrowser<Task> browser = this.solace.browse(queue, Task.class)) {
                return browser.stream(limit).map(AdminRestService::describe).toList();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Peek at the dead message queue.
     *
     * <p>The one browse that earns its keep in normal operation: these are the messages the
     * application gave up on, and nothing else can show you what they were.</p>
     *
     * @param limit how many messages to return
     * @return a summary of each dead message
     */
    @GetMapping("/dmq/peek")
    public Mono<List<Map<String, Object>>> peekDeadMessages(
            @RequestParam(defaultValue = "10") int limit) {
        return Mono.fromCallable(() -> {
            BrowseSpec spec = BrowseSpec.of("#DEAD_MSG_QUEUE");
            try (SolaceBrowser<Object> browser = this.solace.browse(spec, Object.class)) {
                return browser.stream(limit).map(AdminRestService::describe).toList();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Summarise one browsed message.
     *
     * @param record the browsed message
     * @return the fields an operator wants to see
     */
    private static Map<String, Object> describe(SolaceRecord<?> record) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("destination", record.getDestination());
        summary.put("correlationId", record.getCorrelationId());
        summary.put("redelivered", record.isRedelivered());
        summary.put("deliveryCount", record.isDeliveryCountSupported()
                ? record.getDeliveryCount()
                : "unsupported");
        summary.put("payload", String.valueOf(record.getPayload()));
        return summary;
    }
}
