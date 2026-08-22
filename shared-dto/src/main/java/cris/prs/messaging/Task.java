package cris.prs.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Work item for the point-to-point demo: each task is processed by exactly one worker instance,
 * however many are running.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Task {

    private String id;

    private String description;
}
