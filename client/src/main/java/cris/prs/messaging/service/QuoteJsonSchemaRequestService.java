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
 * Request-reply producer for the <b>JSON Schema</b> quote demo.
 *
 * <p>Line for line the same as {@link QuoteRequestService}: it sends a {@link Person} and expects a
 * {@link Quote}. What differs is only where it sends them. The request topic is governed by the schema
 * registry, and a governed topic whose mapping names no format is JSON Schema &mdash; so the converter
 * writes the {@code Person} as the application's own JSON, validated against the registry artifact
 * {@code quote-jsonschema-request}; the {@code Quote} reply comes back the same way on this demo's own
 * reply destination, validated against {@code quote-jsonschema-reply}.</p>
 *
 * <p>Unlike Avro and Protobuf, both artifacts must already exist. They are declared under
 * {@code solace.schema-registry.registration.schemas} and published by the library as this application
 * initialises.</p>
 */
@Service
public class QuoteJsonSchemaRequestService {

    private final ReplyingSolaceTemplate solace;

    private final String requestTopic;

    /**
     * @param solace       the JSON Schema demo's own request-reply template
     * @param requestTopic the JSON Schema quote request topic
     */
    public QuoteJsonSchemaRequestService(
            @Qualifier("quoteJsonSchemaReplyingSolaceTemplate") ReplyingSolaceTemplate solace,
            @Value("${app.quote-jsonschema.topic:request-reply/quote-jsonschema/request}") String requestTopic) {
        this.solace = solace;
        this.requestTopic = requestTopic;
    }

    /** Publish one JSON Schema quote request and return a future for its reply. */
    public RequestReplyFuture<Quote> send(Person person) {
        return this.solace.sendAndReceive(this.requestTopic, person, Quote.class);
    }

    /**
     * Publish several JSON Schema quote requests in one Solace local transaction.
     *
     * @return futures for the replies, to be awaited after this method has returned
     */
    @Transactional
    public List<RequestReplyFuture<Quote>> sendBatchInTransaction(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /** The destination the JSON Schema replies arrive on. */
    public String getReplyDestination() {
        return this.solace.getReplyDestination();
    }
}
