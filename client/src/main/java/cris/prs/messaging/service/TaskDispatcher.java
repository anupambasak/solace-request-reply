package cris.prs.messaging.service;

import cris.prs.messaging.Task;
import cris.prs.messaging.solace.core.SolaceTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Point-to-point producer.
 *
 * <p>Each task is picked up by exactly one worker, because the workers share a single non-exclusive
 * endpoint. {@link #dispatchBatch} publishes inside a Solace local transaction, so a batch either
 * reaches the queue whole or not at all.</p>
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

    /** Submit several tasks atomically: all of them are released at commit, or none are. */
    @Transactional
    public List<Task> dispatchBatch(List<String> descriptions) {
        return descriptions.stream().map(this::dispatch).toList();
    }
}
