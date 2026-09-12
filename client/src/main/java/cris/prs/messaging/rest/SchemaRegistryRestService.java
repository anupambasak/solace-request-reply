package cris.prs.messaging.rest;

import cris.prs.messaging.Quote;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.proto.QuoteProtoMapper;
import cris.prs.messaging.service.PersonFactory;
import cris.prs.messaging.service.QuoteAvroRequestService;
import cris.prs.messaging.service.QuoteJsonSchemaRequestService;
import cris.prs.messaging.service.QuoteProtobufRequestService;
import lombok.extern.slf4j.Slf4j;
import org.cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Request-reply over <b>Apicurio Registry</b>: the quote service with its payloads as Avro, as Protobuf
 * and as schema-validated JSON.
 *
 * <p>All three answer the same question as {@code /request-reply/quote/*} &mdash; a {@code Person} in, a
 * {@link Quote} out &mdash; and all three return the shared {@code Quote} DTO, so they can be compared
 * side by side. Only the wire format differs:</p>
 * <ul>
 *   <li>{@code /request-reply/quote-avro/*} &mdash; the DTOs themselves, as Avro by reflection;</li>
 *   <li>{@code /request-reply/quote-protobuf/*} &mdash; generated {@code QuoteRequest} /
 *       {@code QuoteReply} messages, mapped from and back to the DTOs;</li>
 *   <li>{@code /request-reply/quote-jsonschema/*} &mdash; the DTOs as ordinary JSON, validated against a
 *       registered JSON Schema.</li>
 * </ul>
 *
 * <p>Every body carries Apicurio's framing (magic byte, schema id) and a {@code schemaFormat} property.
 * The Avro and Protobuf artifacts appear on the first request of each kind, since the demo registry has
 * {@code auto-register} on; the JSON Schema ones cannot be inferred from data and are declared under
 * {@code solace.schema-registry.registration.schemas}, which the library publishes at startup. Each demo
 * has its own reply destination &mdash;
 * {@code GET /request-reply/schema-registry/reply-destination} shows them.</p>
 */
@Slf4j
@RestController
@RequestMapping("/request-reply")
public class SchemaRegistryRestService {

    private final QuoteAvroRequestService avro;

    private final QuoteProtobufRequestService protobuf;

    private final QuoteJsonSchemaRequestService jsonSchema;

    private final PersonFactory personFactory;

    /**
     * @param avro          the Avro quote requester
     * @param protobuf      the Protobuf quote requester
     * @param jsonSchema    the JSON Schema quote requester
     * @param personFactory sample payloads, the same ones every other quote endpoint uses
     */
    public SchemaRegistryRestService(QuoteAvroRequestService avro, QuoteProtobufRequestService protobuf,
            QuoteJsonSchemaRequestService jsonSchema, PersonFactory personFactory) {
        this.avro = avro;
        this.protobuf = protobuf;
        this.jsonSchema = jsonSchema;
        this.personFactory = personFactory;
    }

    /**
     * The reply destinations of the three schema-registry demos.
     *
     * @return each demo's reply destination, ending in this pod's id
     */
    @GetMapping("/schema-registry/reply-destination")
    public Mono<Map<String, String>> replyDestination() {
        Map<String, String> destinations = new LinkedHashMap<>();
        destinations.put("quote-avro", avro.getReplyDestination());
        destinations.put("quote-protobuf", protobuf.getReplyDestination());
        destinations.put("quote-jsonschema", jsonSchema.getReplyDestination());
        return Mono.just(destinations);
    }

    // --- Avro: the shared DTOs, by reflection ------------------------------------------------

    /**
     * One Avro quote exchange.
     *
     * @return the quote with its send time, receive time and latency
     */
    @GetMapping("/quote-avro/send")
    public Mono<ReplyResult<Quote>> sendAvro() {
        return await(avro.send(personFactory.create()), Function.identity());
    }

    /**
     * Several Avro quote exchanges as independent publishes.
     *
     * @param count       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each quote as it arrives
     */
    @GetMapping("/quote-avro/send-multiple")
    public Flux<ReplyResult<Quote>> sendAvroMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(personFactory.create(count))
                .flatMap(person -> await(avro.send(person), Function.identity())
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several Avro quote requests published in one Solace transaction.
     *
     * @param count how many requests to publish in the transaction
     * @return every quote, once all of them have arrived
     */
    @GetMapping("/quote-avro/send-batch")
    public Mono<List<ReplyResult<Quote>>> sendAvroBatch(@RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> avro.sendBatchInTransaction(personFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(futures -> Flux.fromIterable(futures)
                        .flatMap(future -> await(future, Function.<Quote>identity())).collectList());
    }

    // --- Protobuf: generated messages, mapped to and from the DTOs ---------------------------

    /**
     * One Protobuf quote exchange.
     *
     * @return the quote, mapped back to the shared DTO, with its timings
     */
    @GetMapping("/quote-protobuf/send")
    public Mono<ReplyResult<Quote>> sendProtobuf() {
        return await(protobuf.send(personFactory.create()), QuoteProtoMapper::toQuote);
    }

    /**
     * Several Protobuf quote exchanges as independent publishes.
     *
     * @param count       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each quote as it arrives
     */
    @GetMapping("/quote-protobuf/send-multiple")
    public Flux<ReplyResult<Quote>> sendProtobufMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(personFactory.create(count))
                .flatMap(person -> await(protobuf.send(person), QuoteProtoMapper::toQuote)
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several Protobuf quote requests published in one Solace transaction.
     *
     * @param count how many requests to publish in the transaction
     * @return every quote, once all of them have arrived
     */
    @GetMapping("/quote-protobuf/send-batch")
    public Mono<List<ReplyResult<Quote>>> sendProtobufBatch(@RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> protobuf.sendBatchInTransaction(personFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(futures -> Flux.fromIterable(futures)
                        .flatMap(future -> await(future, QuoteProtoMapper::toQuote)).collectList());
    }

    // --- JSON Schema: the shared DTOs as plain JSON, validated against a registered schema ---

    /**
     * One JSON Schema quote exchange.
     *
     * @return the quote with its send time, receive time and latency
     */
    @GetMapping("/quote-jsonschema/send")
    public Mono<ReplyResult<Quote>> sendJsonSchema() {
        return await(jsonSchema.send(personFactory.create()), Function.identity());
    }

    /**
     * Several JSON Schema quote exchanges as independent publishes.
     *
     * @param count       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each quote as it arrives
     */
    @GetMapping("/quote-jsonschema/send-multiple")
    public Flux<ReplyResult<Quote>> sendJsonSchemaMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(personFactory.create(count))
                .flatMap(person -> await(jsonSchema.send(person), Function.identity())
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several JSON Schema quote requests published in one Solace transaction.
     *
     * @param count how many requests to publish in the transaction
     * @return every quote, once all of them have arrived
     */
    @GetMapping("/quote-jsonschema/send-batch")
    public Mono<List<ReplyResult<Quote>>> sendJsonSchemaBatch(@RequestParam(defaultValue = "5") int count) {
        return Mono.fromCallable(() -> jsonSchema.sendBatchInTransaction(personFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(futures -> Flux.fromIterable(futures)
                        .flatMap(future -> await(future, Function.<Quote>identity())).collectList());
    }

    /**
     * Await one reply, convert it, and pair it with the timings recorded on its future.
     *
     * @param future  the outstanding request
     * @param convert turns the reply into what the endpoint returns
     * @param <R>     the reply type on the wire
     * @param <T>     the type returned
     * @return the converted reply with its timings
     */
    private <R, T> Mono<ReplyResult<T>> await(RequestReplyFuture<R> future, Function<R, T> convert) {
        return Mono.fromFuture(future)
                .map(reply -> new ReplyResult<>(convert.apply(reply), future.getSendTime(), future.getReceiveTime()));
    }
}
