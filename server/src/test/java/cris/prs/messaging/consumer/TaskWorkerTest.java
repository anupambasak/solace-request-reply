package cris.prs.messaging.consumer;

import cris.prs.messaging.Task;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Point-to-point consumer: each task this instance is given is processed once. */
class TaskWorkerTest {

    @Test
    @DisplayName("processes each task it is handed")
    void processesEachTask() {
        TaskWorker worker = new TaskWorker();

        worker.onTask(new Task("t-1", "reindex"));
        worker.onTask(new Task("t-2", "rebuild"));

        assertThat(worker.getProcessedCount()).isEqualTo(2);
        assertThat(worker.getLastTask().get().getDescription()).isEqualTo("rebuild");
    }
}
