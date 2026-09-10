package cris.prs.messaging.service;

import cris.prs.messaging.Task;
import org.cris.prs.messaging.solace.core.SolaceTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * Point-to-point producer.
 *
 * <p>Each task is picked up by exactly one worker, because the workers share a single non-exclusive
 * endpoint. The producer does nothing to arrange that &mdash; it publishes to a topic like any
 * other producer.</p>
 */
@Slf4j
@Service
public class TaskDispatcher {

    private final SolaceTemplate<Object> solace;

    private final String topic;

    public TaskDispatcher(SolaceTemplate<Object> solace,
            @Value("${app.task.topic:task/submit}") String topic) {
        this.solace = solace;
        this.topic = topic;
    }

    /** Submit one task for whichever worker takes it. */
    public Task dispatch(String description) {
        Task task = new Task(UUID.randomUUID().toString(), description);
        this.solace.send(this.topic, task);
        log.debug("Dispatched task {} to {}", task.getId(), this.topic);
        return task;
    }

    /** Submit several tasks as independent publishes; each is queued as it is sent. */
    public List<Task> dispatchMultiple(String description, int count) {
        return descriptions(description, count).stream().map(this::dispatch).toList();
    }

    /** Submit several tasks atomically: all of them are released at commit, or none are. */
    @Transactional
    public List<Task> dispatchBatch(String description, int count) {
        return descriptions(description, count).stream().map(this::dispatch).toList();
    }

    /** Submit the given descriptions atomically. */
    @Transactional
    public List<Task> dispatchBatch(List<String> descriptions) {
        return descriptions.stream().map(this::dispatch).toList();
    }

    private List<String> descriptions(String description, int count) {
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> description + " (" + i + " of " + count + ")")
                .toList();
    }
}
