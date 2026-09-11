package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import org.cris.prs.messaging.solace.annotation.SolaceListener;
import org.cris.prs.messaging.solace.core.SolaceHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Second request-reply service, independent of {@link ServiceConsumer}.
 *
 * <p>Bound to the durable queue {@code request-reply-queue-2.request-reply-group-2}, subscribed to
 * {@code request-reply/request-2} and {@code request-reply/request-2/>}.</p>
 *
 * <p><b>Its own request topic matters as much as its own queue.</b> Two queues subscribed to the
 * same topic each receive a copy of every request, so both services would answer and the requester
 * would see one reply and one orphan. Separate services need separate request topics.</p>
 *
 * <p>Replies go to the same per-instance destination as the first service's; the correlation id is
 * what keeps the two conversations apart. This service answers with a {@link Quote} rather than a
 * {@link Person}, which is the point of having two: the reply type is chosen per call by the
 * requester.</p>
 */
@Slf4j
@Component
public class QuoteConsumer {

    @SolaceListener(
            id = "quote",
            pattern = "REQUEST_REPLY",
            queue = "${app.quote.queue:request-reply-queue-2}",
            group = "${app.quote.group:request-reply-group-2}",
            topics = {"${app.quote.topic:request-reply/request-2}", "${app.quote.topic:request-reply/request-2}/>"},
            concurrency = "${app.quote.concurrency:5}",
            transactional = "${app.quote.transactional:true}")
    public Quote quote(Person person,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling quote correlationId={} payload={}", correlationId, person);
        Quote quote = QuotePricing.quoteFor(person);
        log.debug("Quoted {} {} for {}", quote.getAmount(), quote.getCurrency(), quote.getPersonName());
        return quote;
    }
}
