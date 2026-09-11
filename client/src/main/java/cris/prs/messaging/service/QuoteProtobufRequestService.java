package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import cris.prs.messaging.proto.QuoteProtoMapper;
import cris.prs.messaging.proto.QuoteReply;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Request-reply producer for the <b>Protobuf</b> quote demo.
 *
 * <p>Protobuf needs generated messages on the wire, so the {@link Person} is converted to a
 * {@code QuoteRequest} by {@link QuoteProtoMapper} and the reply arrives as a {@link QuoteReply}; callers
 * turn it back into the shared {@code Quote} with {@link QuoteProtoMapper#toQuote}. The converter
 * recognises both as Protobuf by type, with no format setting, and validates them against the registry
 * artifacts {@code quote-protobuf-request} and {@code quote-protobuf-reply}.</p>
 */
@Service
public class QuoteProtobufRequestService {

    private final ReplyingSolaceTemplate solace;

    private final String requestTopic;

    /**
     * @param solace       the Protobuf demo's own request-reply template
     * @param requestTopic the Protobuf quote request topic
     */
    public QuoteProtobufRequestService(
            @Qualifier("quoteProtobufReplyingSolaceTemplate") ReplyingSolaceTemplate solace,
            @Value("${app.quote-protobuf.topic:request-reply/quote-protobuf/request}") String requestTopic) {
        this.solace = solace;
        this.requestTopic = requestTopic;
    }

    /** Publish one Protobuf quote request and return a future for its reply. */
    public RequestReplyFuture<QuoteReply> send(Person person) {
        return this.solace.sendAndReceive(this.requestTopic, QuoteProtoMapper.toRequest(person), QuoteReply.class);
    }

    /**
     * Publish several Protobuf quote requests in one Solace local transaction.
     *
     * @return futures for the replies, to be awaited after this method has returned
     */
    @Transactional
    public List<RequestReplyFuture<QuoteReply>> sendBatchInTransaction(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /** The destination the Protobuf replies arrive on. */
    public String getReplyDestination() {
        return this.solace.getReplyDestination();
    }
}
