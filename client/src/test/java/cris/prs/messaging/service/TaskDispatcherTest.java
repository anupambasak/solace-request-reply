package cris.prs.messaging.service;

import cris.prs.messaging.Task;
import cris.prs.messaging.solace.core.SolaceTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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

    private TaskDispatcher dispatcher() {
        return new TaskDispatcher(this.solaceTemplate, TOPIC);
    }

    @Nested
    @DisplayName("single")
    class Single {

        @Test
        @DisplayName("dispatches a single task to the work topic")
        void dispatchesASingleTask() {
            Task task = dispatcher().dispatch("reindex catalogue");

            assertThat(task.getDescription()).isEqualTo("reindex catalogue");
            assertThat(task.getId()).isNotBlank();
            verify(solaceTemplate).send(eq(TOPIC), any(Task.class));
        }

        @Test
        @DisplayName("gives every task a distinct id, since exactly one worker will claim each")
        void assignsADistinctIdPerTask() {
            TaskDispatcher dispatcher = dispatcher();

            assertThat(dispatcher.dispatch("one").getId())
                    .isNotEqualTo(dispatcher.dispatch("two").getId());
        }
    }

    @Nested
    @DisplayName("multiple")
    class Multiple {

        @Test
        @DisplayName("publishes one message per task")
        void publishesOnePerTask() {
            List<Task> tasks = dispatcher().dispatchMultiple("reindex", 3);

            assertThat(tasks).hasSize(3)
                    .extracting(Task::getDescription)
                    .containsExactly("reindex (1 of 3)", "reindex (2 of 3)", "reindex (3 of 3)");
            verify(solaceTemplate, times(3)).send(eq(TOPIC), any(Task.class));
        }
    }

    @Nested
    @DisplayName("batch")
    class Batch {

        @Test
        @DisplayName("publishes one message per task, for the transaction to release together")
        void publishesOnePerTask() {
            List<Task> tasks = dispatcher().dispatchBatch("rebuild", 4);

            assertThat(tasks).hasSize(4);
            verify(solaceTemplate, times(4)).send(eq(TOPIC), any(Task.class));
        }

        @Test
        @DisplayName("accepts explicit descriptions")
        void acceptsExplicitDescriptions() {
            List<Task> tasks = dispatcher().dispatchBatch(List.of("one", "two", "three"));

            assertThat(tasks).extracting(Task::getDescription).containsExactly("one", "two", "three");
            verify(solaceTemplate, times(3)).send(eq(TOPIC), any(Task.class));
        }
    }
}
