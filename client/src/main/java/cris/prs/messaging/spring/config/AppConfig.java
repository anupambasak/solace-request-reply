package cris.prs.messaging.spring.config;

import org.cris.prs.messaging.solace.core.EndpointMode;
import org.cris.prs.messaging.solace.requestreply.ReplyEndpointSpec;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplateFactory;
import org.cris.prs.messaging.solace.transaction.SolaceTransactionManager;
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
        return factory.create(replyEndpoint("inventoryReplyContainer", replyTopicPrefix, concurrency,
                replyTimeout));
    }

    /**
     * The reply destination of the Avro quote demo, {@code request-reply/quote-avro/reply/<pod>}.
     *
     * <p>The schema-registry demos each get their own, rather than the shared one, because a reply
     * destination is also a registry mapping: {@code request-reply/quote-avro/reply/>} names the one
     * artifact every pod's Avro replies are validated against. A shared destination would carry
     * {@code Person} and {@code Quote} replies in JSON, Avro and Protobuf at once, which no single
     * topic mapping can describe.</p>
     *
     * @param factory          builds the template and the container consuming its reply destination
     * @param replyTopicPrefix base reply topic; the instance id is appended to it
     * @param concurrency      flows consuming the replies; clamped to 1 on a non-durable endpoint
     * @param replyTimeout     how long a request waits for its reply
     * @return the Avro quote request-reply template
     */
    @Bean
    public ReplyingSolaceTemplate quoteAvroReplyingSolaceTemplate(
            ReplyingSolaceTemplateFactory factory,
            @Value("${app.quote-avro.reply-topic-prefix:request-reply/quote-avro/reply}") String replyTopicPrefix,
            @Value("${app.quote-avro.reply-concurrency:1}") int concurrency,
            @Value("${app.quote-avro.reply-timeout:30s}") Duration replyTimeout) {
        return factory.create(replyEndpoint("quoteAvroReplyContainer", replyTopicPrefix, concurrency, replyTimeout));
    }

    /**
     * The reply destination of the Protobuf quote demo, {@code request-reply/quote-protobuf/reply/<pod>};
     * its own for the same reason as {@link #quoteAvroReplyingSolaceTemplate}'s.
     *
     * @param factory          builds the template and the container consuming its reply destination
     * @param replyTopicPrefix base reply topic; the instance id is appended to it
     * @param concurrency      flows consuming the replies; clamped to 1 on a non-durable endpoint
     * @param replyTimeout     how long a request waits for its reply
     * @return the Protobuf quote request-reply template
     */
    @Bean
    public ReplyingSolaceTemplate quoteProtobufReplyingSolaceTemplate(
            ReplyingSolaceTemplateFactory factory,
            @Value("${app.quote-protobuf.reply-topic-prefix:request-reply/quote-protobuf/reply}") String replyTopicPrefix,
            @Value("${app.quote-protobuf.reply-concurrency:1}") int concurrency,
            @Value("${app.quote-protobuf.reply-timeout:30s}") Duration replyTimeout) {
        return factory.create(replyEndpoint("quoteProtobufReplyContainer", replyTopicPrefix, concurrency,
                replyTimeout));
    }

    /**
     * The reply destination of the JSON Schema quote demo,
     * {@code request-reply/quote-jsonschema/reply/<pod>}; its own for the same reason as
     * {@link #quoteAvroReplyingSolaceTemplate}'s.
     *
     * @param factory          builds the template and the container consuming its reply destination
     * @param replyTopicPrefix base reply topic; the instance id is appended to it
     * @param concurrency      flows consuming the replies; clamped to 1 on a non-durable endpoint
     * @param replyTimeout     how long a request waits for its reply
     * @return the JSON Schema quote request-reply template
     */
    @Bean
    public ReplyingSolaceTemplate quoteJsonSchemaReplyingSolaceTemplate(
            ReplyingSolaceTemplateFactory factory,
            @Value("${app.quote-jsonschema.reply-topic-prefix:request-reply/quote-jsonschema/reply}")
            String replyTopicPrefix,
            @Value("${app.quote-jsonschema.reply-concurrency:1}") int concurrency,
            @Value("${app.quote-jsonschema.reply-timeout:30s}") Duration replyTimeout) {
        return factory.create(replyEndpoint("quoteJsonSchemaReplyContainer", replyTopicPrefix, concurrency,
                replyTimeout));
    }

    /**
     * One reply destination, with requests expiring when their requester stops waiting.
     *
     * <p>{@code timeToLive} is the part worth copying. A request is persistent and its endpoint is
     * durable, so it outlives the responder: restart the server mid-flight and the request waits on the
     * queue, gets handled whenever the server comes back, and the reply arrives to a requester that gave
     * up minutes ago &mdash; {@code Received a reply with no outstanding request}. Expiring the request at
     * the reply timeout makes the broker stop delivering it at the moment the requester stops caring, and
     * (the endpoints are provisioned {@code respects-ttl} and DMQ-eligible) park it on the dead message
     * queue where it can be inspected, rather than answering it far too late.</p>
     *
     * <p>Unset values &mdash; priority, DMQ eligibility &mdash; come from {@code solace.template.*}.</p>
     */
    private static ReplyEndpointSpec replyEndpoint(String id, String replyTopicPrefix, int concurrency,
            Duration replyTimeout) {
        ReplyEndpointSpec spec = new ReplyEndpointSpec();
        spec.setId(id);
        spec.setReplyTopicPrefix(replyTopicPrefix);
        spec.setAppendInstanceId(true);
        spec.setEndpointMode(EndpointMode.NON_DURABLE_QUEUE);
        spec.setConcurrency(concurrency);
        spec.setReplyTimeout(replyTimeout);
        spec.setTimeToLive(replyTimeout.toMillis());
        return spec;
    }
}
