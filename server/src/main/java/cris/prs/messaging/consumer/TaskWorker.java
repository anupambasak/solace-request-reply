package cris.prs.messaging.consumer;

import cris.prs.messaging.Task;
import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.SolaceRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Point-to-point consumer.
 *
 * <p>{@code POINT_TO_POINT} binds every instance to the one shared, durable, non-exclusive endpoint
 * {@code task.workers}, so each task is handled exactly once no matter how many servers run.
 * Scaling out adds throughput instead of duplicating work.</p>
 *
 * <p>Consumed transactionally: the acknowledgement commits only once the task has been processed,
 * so a crash mid-task returns the message to the queue for another worker.</p>
 *
 * <p>Takes a {@code SolaceRecord} rather than the bare payload so the delivery count is visible. On a
 * transacted flow a rollback returns the message and the broker increments that count, so a value
 * above 1 is the signal that this task has already failed at least once &mdash; the hook for
 * idempotence, and for giving up before {@code max-redelivery-count} sends it to the dead message
 * queue.</p>
 */
@Slf4j
@Component
public class TaskWorker {

    private final AtomicInteger processedCount = new AtomicInteger();

    @Getter
    private final AtomicReference<Task> lastTask = new AtomicReference<>();

    @SolaceListener(
            id = "taskWorker",
            pattern = "POINT_TO_POINT",
            queue = "${app.task.queue:task}",
            group = "${app.task.group:workers}",
            topics = {"${app.task.topic:task/submit}"},
            concurrency = "${app.task.concurrency:5}",
            transactional = "${app.task.transactional:true}")
    public void onTask(SolaceRecord<Task> record) {
        Task task = record.getPayload();
        int count = this.processedCount.incrementAndGet();
        this.lastTask.set(task);
        if (record.isDeliveryCountSupported() && record.getDeliveryCount() > 1) {
            log.warn("Task {} is on delivery attempt {}", task.getId(), record.getDeliveryCount());
        }
        log.info("Task {} processed (total {}): {}", task.getId(), count, task.getDescription());
    }

    public int getProcessedCount() {
        return this.processedCount.get();
    }
}
