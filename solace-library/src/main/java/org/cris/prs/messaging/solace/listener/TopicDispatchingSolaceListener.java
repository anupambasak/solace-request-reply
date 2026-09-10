package org.cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.cris.prs.messaging.solace.support.SolaceTopicMatcher;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Routes each message to the listener whose subscription matched its topic.
 *
 * <p>Without this, every {@code @SolaceListener} gets its own endpoint and its own flows: a service
 * subscribing to twenty related topics pays for twenty queues, twenty binds and twenty sets of
 * provisioning. Topic dispatch lets those methods share one endpoint, with the union of their
 * subscriptions on it, and fans the delivered messages back out by topic.</p>
 *
 * <p>The broker decided <em>that</em> a message belonged on the endpoint; this decides <em>which</em>
 * method wanted it, by matching the message's destination against each target's subscriptions with
 * {@link SolaceTopicMatcher}. Matching is client-side because JCSMP's native topic dispatch is
 * reachable only through an internal {@code impl} package.</p>
 *
 * <h2>Order matters</h2>
 * <p>The first target whose subscription matches wins, and targets are ordered as their methods were
 * registered. Overlapping subscriptions are therefore resolved by declaration order, not by
 * specificity: if one method takes {@code orders/&gt;} and another {@code orders/created}, whichever
 * was registered first receives {@code orders/created}. Declare the specific ones first, or do not
 * overlap.</p>
 */
@Slf4j
public class TopicDispatchingSolaceListener implements SolaceMessageListener {

    private final String listenerId;

    private final List<Target> targets;

    /**
     * Create the dispatcher.
     *
     * @param listenerId the container's id, for log messages
     * @param targets    the routing table, in declaration order
     */
    public TopicDispatchingSolaceListener(String listenerId, List<Target> targets) {
        this.listenerId = listenerId;
        this.targets = List.copyOf(targets);
    }

    /**
     * {@inheritDoc}
     *
     * <p>A message matching no target is logged at warning and treated as handled, so the container
     * acknowledges it. Failing it instead would redeliver forever: nothing about a redelivery makes a
     * subscription match that did not match the first time, and the endpoint would fill with messages
     * no method wants. The warning is the signal that the endpoint carries a subscription no target
     * claims &mdash; usually a leftover on a durable queue from an earlier version of the code, since
     * a durable endpoint keeps its subscriptions across restarts.</p>
     */
    @Override
    public void onMessage(BytesXMLMessage message) throws Exception {
        String topic = message.getDestination() != null ? message.getDestination().getName() : null;
        for (Target target : this.targets) {
            if (target.matches(topic)) {
                target.listener().onMessage(message);
                return;
            }
        }
        log.warn("Container '{}' received a message on '{}' that no listener subscribes to; "
                + "acknowledging it. A durable endpoint keeps subscriptions across restarts, so this "
                + "is usually one left behind by an earlier version of the code.",
                this.listenerId, topic);
    }

    /**
     * One entry in the routing table.
     *
     * @param topics      the subscriptions claimed by this target
     * @param listener    the adapter to invoke
     * @param description bean and method, for log messages
     */
    public record Target(List<String> topics, SolaceMessageListener listener, String description) {

        /**
         * Whether this target claims a topic.
         *
         * @param topic the message's destination, possibly {@code null}
         * @return {@code true} when any of this target's subscriptions matches
         */
        public boolean matches(String topic) {
            if (topic == null) {
                return false;
            }
            return this.topics.stream().anyMatch(pattern -> SolaceTopicMatcher.matches(pattern, topic));
        }
    }
}
