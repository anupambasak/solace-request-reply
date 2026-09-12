package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.annotation.SolaceListener;
import org.cris.prs.messaging.solace.core.SolaceHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * The quote service once more, with request and reply as <b>JSON Schema</b> governed by Apicurio Registry.
 *
 * <p>The bytes on the wire are the JSON the plain {@link QuoteConsumer} would have sent, written by the
 * application's own {@code ObjectMapper} &mdash; plus Apicurio's framing and a schema that says what the
 * JSON must look like. So this is the format to reach for when payloads are already JSON and what is
 * missing is the contract, rather than a smaller encoding.</p>
 *
 * <p>Two things differ from the Avro and Protobuf demos:</p>
 * <ul>
 *   <li>the schemas are <em>registered before the demo runs</em> &mdash; JSON Schema is the one format
 *       Apicurio cannot derive from the data, so {@code auto-register} has nothing to work from. Both
 *       applications declare them under {@code solace.schema-registry.registration.schemas} with
 *       {@code mode: STARTUP}, and the library publishes {@code shared-dto}'s
 *       {@code classpath:schemas/quote-jsonschema-*.json} as they initialise;</li>
 *   <li>the serde deserialises to a Jackson {@code JsonNode}, which the converter maps onto this method's
 *       {@link Person} with the same mapper &mdash; which is why neither DTO needs a {@code javaType} in
 *       its schema, or any schema dependency at all.</li>
 * </ul>
 */
@Slf4j
@Component
public class QuoteJsonSchemaConsumer {

    /**
     * Quote for the person in a JSON Schema request, answered the same way.
     *
     * @param person        validated against {@code quote-jsonschema-request} on the way in
     * @param correlationId the request's correlation id, for the log
     * @return the quote, validated against {@code quote-jsonschema-reply} on the way out
     */
    @SolaceListener(
            id = "quoteJsonSchema",
            pattern = "REQUEST_REPLY",
            queue = "${app.quote-jsonschema.queue:request-reply-queue-6}",
            group = "${app.quote-jsonschema.group:request-reply-group-6}",
            topics = "${app.quote-jsonschema.topic:request-reply/quote-jsonschema/request}",
            concurrency = "${app.quote-jsonschema.concurrency:5}",
            transactional = "${app.quote-jsonschema.transactional:true}")
    public Quote quote(Person person,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling JSON Schema quote correlationId={} payload={}", correlationId, person);
        return QuotePricing.quoteFor(person);
    }
}
