package cris.prs.messaging.service;

import cris.prs.messaging.Task;
import cris.prs.messaging.solace.core.SolaceTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Point-to-point producer: one send per task, all addressed to the shared work topic. */
@ExtendWith(MockitoExtension.class)
class TaskDispatcherTest {

    private static final String TOPIC = "task/submit";

    @Mock
    private SolaceTemplate<Object> solaceTemplate;

    @Test
    @DisplayName("dispatches a single task to the work topic")
    void dispatchesASingleTask() {
        TaskDispatcher dispatcher = new TaskDispatcher(this.solaceTemplate, TOPIC);

        Task task = dispatcher.dispatch("reindex catalogue");

        assertThat(task.getDescription()).isEqualTo("reindex catalogue");
        assertThat(task.getId()).isNotBlank();
        verify(this.solaceTemplate).send(eq(TOPIC), any(Task.class));
    }

    @Test
    @DisplayName("publishes one message per task in a batch")
    void publishesEveryTaskInABatch() {
        TaskDispatcher dispatcher = new TaskDispatcher(this.solaceTemplate, TOPIC);

        List<Task> tasks = dispatcher.dispatchBatch(List.of("one", "two", "three"));

        assertThat(tasks).hasSize(3)
                .extracting(Task::getDescription)
                .containsExactly("one", "two", "three");
        verify(this.solaceTemplate, times(3)).send(eq(TOPIC), any(Task.class));
    }
}
