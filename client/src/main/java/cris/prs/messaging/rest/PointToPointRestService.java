package cris.prs.messaging.rest;

import cris.prs.messaging.Task;
import cris.prs.messaging.service.TaskDispatcher;
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
 * Point-to-point endpoints.
 *
 * <p>Every task submitted here is handled by <em>exactly one</em> server instance, because the
 * workers share a single non-exclusive endpoint. Scale the server up and the tasks spread across
 * the pods instead of being repeated in each.</p>
 *
 * <p>The three endpoints differ in how the messages are published: one task, several independent
 * publishes, or several publishes in a single Solace transaction.</p>
 */
@Slf4j
@RestController
@RequestMapping("/point-to-point")
@RequiredArgsConstructor
public class PointToPointRestService {

    private final TaskDispatcher taskDispatcher;

    /**
     * Submit a single task.
     *
     * @param description what the task is
     * @return the task that was queued
     */
    @GetMapping("/submit")
    public Mono<Task> submit(@RequestParam(defaultValue = "process booking") String description) {
        return Mono.fromCallable(() -> taskDispatcher.dispatch(description))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Submit several tasks as independent publishes.
     *
     * <p>Each is queued as it is sent, so workers can start on the first while the rest are still
     * being published.</p>
     *
     * @param description what each task is, suffixed with its position
     * @param count       how many to submit
     * @return the tasks that were queued, streamed as they complete
     */
    @GetMapping("/submit-multiple")
    public Flux<Task> submitMultiple(
            @RequestParam(defaultValue = "process booking") String description,
            @RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> taskDispatcher.dispatchMultiple(description, count))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(Flux::fromIterable);
    }

    /**
     * Submit several tasks in one Solace local transaction.
     *
     * <p>Nothing is queued until the transaction commits, so no worker can start on a partial
     * batch.</p>
     *
     * @param description what each task is, suffixed with its position
     * @param count       how many to submit in the transaction
     * @return the tasks that were queued
     */
    @GetMapping("/submit-batch")
    public Mono<List<Task>> submitBatch(
            @RequestParam(defaultValue = "process booking") String description,
            @RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> taskDispatcher.dispatchBatch(description, count))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
