package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.annotation.SolaceListener;
import org.cris.prs.messaging.solace.core.SolaceHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * The quote service again, with request and reply as <b>Apache Avro</b> governed by Apicurio Registry.
 *
 * <p>The listener is the same shape as {@link QuoteConsumer}'s &mdash; {@link Person} in, {@link Quote}
 * out, the shared DTOs unchanged. Everything Avro is configuration:</p>
 * <ul>
 *   <li>{@code solace.schema-registry.avro.datum-provider: REFLECT_ALLOW_NULL} lets Avro write and read
 *       plain Java objects, with a schema derived from the class's fields;</li>
 *   <li>the {@code topic-profile} mapping for {@code request-reply/quote-avro/request} says POJOs there
 *       are Avro, and names the artifact the request schema lives under;</li>
 *   <li>the reply goes to the requester's {@code replyTo}, {@code request-reply/quote-avro/reply/<pod>},
 *       whose mapping {@code request-reply/quote-avro/reply/>} does the same for {@code Quote} &mdash; one
 *       artifact for every client instance, which is why the mapping uses {@code >}.</li>
 * </ul>
 *
 * <p>The message itself says it is Avro: the body carries Apicurio's framing (magic byte, schema id) and
 * the {@code schemaFormat} property is {@code AVRO}, so the converter picks the Avro codec before this
 * method is called.</p>
 */
@Slf4j
@Component
public class QuoteAvroConsumer {

    /**
     * Quote for the person in an Avro request, answered in Avro.
     *
     * @param person        decoded from Avro through the registry
     * @param correlationId the request's correlation id, for the log
     * @return the quote, encoded as Avro on its way back
     */
    @SolaceListener(
            id = "quoteAvro",
            pattern = "REQUEST_REPLY",
            queue = "${app.quote-avro.queue:request-reply-queue-4}",
            group = "${app.quote-avro.group:request-reply-group-4}",
            topics = "${app.quote-avro.topic:request-reply/quote-avro/request}",
            concurrency = "${app.quote-avro.concurrency:5}",
            transactional = "${app.quote-avro.transactional:true}")
    public Quote quote(Person person,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling Avro quote correlationId={} payload={}", correlationId, person);
        return QuotePricing.quoteFor(person);
    }
}
