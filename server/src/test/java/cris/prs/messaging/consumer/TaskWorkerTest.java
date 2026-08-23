package cris.prs.messaging.consumer;

import cris.prs.messaging.Task;
import cris.prs.messaging.solace.core.SolaceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Point-to-point consumer: each task this instance is given is processed once. */
class TaskWorkerTest {

    @Test
    @DisplayName("processes each task it is handed")
    void processesEachTask() {
        TaskWorker worker = new TaskWorker();

        worker.onTask(record(new Task("t-1", "reindex"), 1));
        worker.onTask(record(new Task("t-2", "rebuild"), 1));

        assertThat(worker.getProcessedCount()).isEqualTo(2);
        assertThat(worker.getLastTask().get().getDescription()).isEqualTo("rebuild");
    }

    @Test
    @DisplayName("still processes a redelivered task, and can see which attempt it is")
    void processesARedeliveredTask() {
        TaskWorker worker = new TaskWorker();
        SolaceRecord<Task> retry = record(new Task("t-3", "resend"), 3);

        worker.onTask(retry);

        assertThat(worker.getProcessedCount()).isEqualTo(1);
        assertThat(retry.isDeliveryCountSupported()).isTrue();
        assertThat(retry.getDeliveryCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("an unsupported delivery count is -1, not a first delivery")
    void unsupportedDeliveryCount() {
        SolaceRecord<Task> record = record(new Task("t-4", "legacy"), -1);

        assertThat(record.isDeliveryCountSupported()).isFalse();
        assertThat(record.getDeliveryCount()).isEqualTo(-1);
    }

    private SolaceRecord<Task> record(Task task, int deliveryCount) {
        return new SolaceRecord<>(task, "task/submit", null, null, Map.of(), null, deliveryCount);
    }
}
