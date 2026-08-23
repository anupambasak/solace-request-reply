package cris.prs.messaging.spring.config;

import cris.prs.messaging.solace.core.EndpointMode;
import cris.prs.messaging.solace.requestreply.ReplyEndpointSpec;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplateFactory;
import cris.prs.messaging.solace.transaction.SolaceTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

@Configuration
public class AppConfig {

    /**
     * Programmatic transaction support over the auto-configured Solace transaction manager.
     *
     * @param solaceTransactionManager the auto-configured manager
     * @return a template for running work in a Solace local transaction
     */
    @Bean
    public TransactionTemplate transactionTemplate(SolaceTransactionManager solaceTransactionManager) {
        return new TransactionTemplate(solaceTransactionManager);
    }

    /**
     * A second reply destination, used only by the inventory service.
     *
     * <p>The booking and quote services share the auto-configured {@code replyingSolaceTemplate} and
     * its single per-instance reply destination, which is the right default: the reply channel
     * belongs to the requester and the correlation id returns each reply to its request.</p>
     *
     * <p>Inventory gets its own because it is the case where sharing stops paying: its replies are
     * isolated from the others, so a burst of inventory traffic cannot delay a booking reply behind
     * it on the shared flow, a stalled inventory endpoint cannot stop the other conversations, and
     * its depth and latency can be read on their own.</p>
     *
     * <p>Nothing on the server changes to support this. The responder publishes to whatever
     * {@code replyTo} the request carried, and this template stamps a different one.</p>
     *
     * @param factory          builds the template and the container consuming its reply destination
     * @param replyTopicPrefix base reply topic; the instance id is appended to it
     * @param concurrency      flows consuming inventory replies; a non-durable reply endpoint is a
     *                         temporary queue and accepts exactly one, so anything above 1 is clamped
     * @param replyTimeout     how long an inventory request waits for its reply
     * @return the inventory request-reply template
     */
    @Bean
    public ReplyingSolaceTemplate inventoryReplyingSolaceTemplate(
            ReplyingSolaceTemplateFactory factory,
            @Value("${app.inventory.reply-topic-prefix:request-reply/reply-3}") String replyTopicPrefix,
            @Value("${app.inventory.reply-concurrency:1}") int concurrency,
            @Value("${app.inventory.reply-timeout:30s}") Duration replyTimeout) {
        ReplyEndpointSpec spec = new ReplyEndpointSpec();
        spec.setId("inventoryReplyContainer");
        spec.setReplyTopicPrefix(replyTopicPrefix);
        spec.setAppendInstanceId(true);
        spec.setEndpointMode(EndpointMode.NON_DURABLE_QUEUE);
        spec.setConcurrency(concurrency);
        spec.setReplyTimeout(replyTimeout);
        return factory.create(spec);
    }
}
