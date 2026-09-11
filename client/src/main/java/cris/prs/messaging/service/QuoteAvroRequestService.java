package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Request-reply producer for the <b>Avro</b> quote demo.
 *
 * <p>Line for line the same as {@link QuoteRequestService}: it sends a {@link Person} and expects a
 * {@link Quote}. What differs is only where it sends them. The request topic is governed by the schema
 * registry with POJO format {@code AVRO} ({@code solace.schema-registry.topic-profile}), so the converter
 * writes the {@code Person} as Avro by reflection, validated against the registry artifact
 * {@code quote-avro-request}; the {@code Quote} reply comes back the same way on this demo's own reply
 * destination.</p>
 */
@Service
public class QuoteAvroRequestService {

    private final ReplyingSolaceTemplate solace;

    private final String requestTopic;

    /**
     * @param solace       the Avro demo's own request-reply template
     * @param requestTopic the Avro quote request topic
     */
    public QuoteAvroRequestService(
            @Qualifier("quoteAvroReplyingSolaceTemplate") ReplyingSolaceTemplate solace,
            @Value("${app.quote-avro.topic:request-reply/quote-avro/request}") String requestTopic) {
        this.solace = solace;
        this.requestTopic = requestTopic;
    }

    /** Publish one Avro quote request and return a future for its reply. */
    public RequestReplyFuture<Quote> send(Person person) {
        return this.solace.sendAndReceive(this.requestTopic, person, Quote.class);
    }

    /**
     * Publish several Avro quote requests in one Solace local transaction.
     *
     * @return futures for the replies, to be awaited after this method has returned
     */
    @Transactional
    public List<RequestReplyFuture<Quote>> sendBatchInTransaction(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /** The destination the Avro replies arrive on. */
    public String getReplyDestination() {
        return this.solace.getReplyDestination();
    }
}
