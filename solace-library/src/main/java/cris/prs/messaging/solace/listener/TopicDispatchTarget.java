package cris.prs.messaging.solace.listener;

import lombok.Data;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * One handler method within a topic-dispatching container.
 *
 * <p>A dispatching container binds a single endpoint carrying the union of its targets'
 * subscriptions, and routes each delivered message to the target whose subscription matched. Each
 * target keeps its own payload type, which is the point &mdash; the methods sharing the endpoint
 * take different types.</p>
 */
@Data
public class TopicDispatchTarget {

    /** Create an empty target. */
    public TopicDispatchTarget() {
    }

    /** Subscriptions this target claims. A message matching any of them is routed here. */
    private List<String> topics = new ArrayList<>();

    /** The annotated method to invoke. */
    private InvocableHandlerMethod invocableHandlerMethod;

    /** The type this target's message bodies are converted into. */
    private Class<?> payloadType = Object.class;

    /** Overrides where this target's return value is published; usually empty. */
    private String replyDestination;

    /** Bean and method, for log and error messages. */
    private String description;
}
