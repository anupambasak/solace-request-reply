package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Request-reply producer.
 *
 * <p>Also shows both ways of driving a Solace local transaction from the requesting side.</p>
 *
 * <p><b>The request is only released to the broker when the transaction commits</b>, so a future
 * returned by a transactional method must be awaited <em>after</em> that method returns. Waiting
 * inside the transaction would block on a request that has not been published yet.</p>
 */
@Service
@RequiredArgsConstructor
public class BookingRequestService {

    private final ReplyingSolaceTemplate solace;

    private final TransactionTemplate transactionTemplate;

    @Value("${app.request.topic:request-reply/request-1}")
    private String requestTopic;

    /** Publish one request immediately and return a future for its reply. */
    public RequestReplyFuture<Person> send(Person person) {
        return this.solace.sendAndReceive(this.requestTopic, person, Person.class);
    }

    /** Publish several requests as independent publishes; each is on the wire as it is sent. */
    public List<RequestReplyFuture<Person>> sendMultiple(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /**
     * Declarative transaction: every request in the batch is released together at commit.
     *
     * @return futures for the replies, to be awaited after this method has returned
     */
    @Transactional
    public List<RequestReplyFuture<Person>> sendBatchInTransaction(List<Person> people) {
        return people.stream().map(this::send).toList();
    }

    /** Declarative transaction around a single request. */
    @Transactional
    public RequestReplyFuture<Person> sendInTransaction(Person person) {
        return send(person);
    }

    /** Programmatic equivalent, using a {@code TransactionTemplate}. */
    public RequestReplyFuture<Person> sendInTransactionTemplate(Person person) {
        return this.transactionTemplate.execute(status -> send(person));
    }
}
