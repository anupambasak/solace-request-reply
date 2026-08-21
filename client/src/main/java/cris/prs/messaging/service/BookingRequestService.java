package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Shows both ways of driving a Solace local transaction from the requesting side.
 *
 * <p>Note that the request is only released to the broker when the transaction commits, so the
 * returned future must be waited on <em>after</em> the transactional method returns &mdash; waiting
 * inside the transaction would deadlock against a message that has not been published yet.</p>
 */
@Service
@RequiredArgsConstructor
public class BookingRequestService {

    private final ReplyingSolaceTemplate solace;

    private final TransactionTemplate transactionTemplate;

    /** Declarative: the Solace transaction is begun and committed around this method. */
    @Transactional
    public RequestReplyFuture<Person> sendInTransaction(String topic, Person person) {
        return this.solace.sendAndReceive(topic, person, Person.class);
    }

    /** Programmatic equivalent, using a {@code TransactionTemplate}. */
    public RequestReplyFuture<Person> sendInTransactionTemplate(String topic, Person person) {
        return this.transactionTemplate.execute(status -> this.solace.sendAndReceive(topic, person, Person.class));
    }
}
