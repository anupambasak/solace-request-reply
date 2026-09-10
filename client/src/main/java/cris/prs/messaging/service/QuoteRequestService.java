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
 * Request-reply producer for the second service.
 *
 * <p>Identical in shape to {@link BookingRequestService} but addressed to a different request topic
 * and expecting a different reply type. The two share one {@code ReplyingSolaceTemplate} and one
 * per-instance reply destination: replies are matched by correlation id, so both conversations can
 * be outstanding at the same time.</p>
 */
@Service
public class QuoteRequestService {

    private final ReplyingSolaceTemplate solace;

    private final String requestTopic;

    /**
     * @param solace       the shared request-reply template, the same instance the booking service
     *                     uses; the correlation id keeps the two conversations apart
     * @param requestTopic the quote request topic
     */
    public QuoteRequestService(
            @Qualifier("replyingSolaceTemplate") ReplyingSolaceTemplate solace,
            @Value("${app.quote.topic:request-reply/request-2}") String requestTopic) {
        this.solace = solace;
        this.requestTopic = requestTopic;
    }

    /** Publish one quote request immediately and return a future for its reply. */
    public RequestReplyFuture<Quote> send(Person person) {
        return this.solace.sendAndReceive(this.requestTopic, person, Quote.class);
    }

    /** Publish several quote requests as independent publishes. */
    public List<RequestReplyFuture<Quote>> sendMultiple(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /**
     * Publish several quote requests in one Solace local transaction.
     *
     * @return futures for the replies, to be awaited after this method has returned &mdash; nothing
     *         is on the wire until the transaction commits
     */
    @Transactional
    public List<RequestReplyFuture<Quote>> sendBatchInTransaction(List<Person> people) {
        return people.stream().map(this::send).toList();
    }
}
