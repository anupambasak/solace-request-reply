package cris.prs.messaging.rest;

import cris.prs.messaging.InventoryCheck;
import cris.prs.messaging.InventoryStatus;
import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.service.BookingRequestService;
import cris.prs.messaging.service.InventoryCheckFactory;
import cris.prs.messaging.service.InventoryRequestService;
import cris.prs.messaging.service.PersonFactory;
import cris.prs.messaging.service.QuoteRequestService;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
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

/**
 * Request-reply endpoints.
 *
 * <p>Each request carries a correlation id and a {@code replyTo} naming this pod's own reply
 * destination, so several client replicas can issue requests concurrently and a reply can only
 * reach the instance that asked for it. Every response reports the round-trip latency.</p>
 *
 * <p>The three endpoints differ in how the requests are published: one request, several independent
 * publishes, or several publishes in a single Solace transaction.</p>
 *
 * <p>Two independent request-reply services are exposed here, each with its own request topic and
 * its own durable endpoint on the server, and each answering with a different type. They share one
 * {@code ReplyingSolaceTemplate} and one per-instance reply destination &mdash; the correlation id,
 * not the destination, is what returns each reply to the request that asked for it.</p>
 *
 * <ul>
 *   <li>{@code /request-reply/booking/*} &mdash; replies with a {@code Person}, shared reply destination</li>
 *   <li>{@code /request-reply/quote/*} &mdash; replies with a {@code Quote}, shared reply destination</li>
 *   <li>{@code /request-reply/inventory/*} &mdash; replies with an {@code InventoryStatus}, on its
 *       <b>own</b> reply destination</li>
 * </ul>
 *
 * <p>Inventory is the case where a shared reply destination stops paying: its replies are isolated,
 * so a burst of inventory traffic cannot delay a booking reply behind it, and a stalled inventory
 * endpoint cannot stop the other conversations. {@code GET /request-reply/reply-destination} shows
 * both destinations.</p>
 */
@Slf4j
@RestController
@RequestMapping("/request-reply")
public class RequestReplyRestService {

    private final BookingRequestService bookingRequestService;

    private final QuoteRequestService quoteRequestService;

    private final InventoryRequestService inventoryRequestService;

    private final ReplyingSolaceTemplate solace;

    private final PersonFactory personFactory;

    private final InventoryCheckFactory inventoryCheckFactory;

    /**
     * @param bookingRequestService   service one
     * @param quoteRequestService     service two
     * @param inventoryRequestService service three, on its own reply destination
     * @param solace                  the shared template, qualified because a second
     *                                {@code ReplyingSolaceTemplate} exists
     * @param personFactory           sample payloads for booking and quote
     * @param inventoryCheckFactory   sample payloads for inventory
     */
    public RequestReplyRestService(BookingRequestService bookingRequestService,
            QuoteRequestService quoteRequestService,
            InventoryRequestService inventoryRequestService,
            @Qualifier("replyingSolaceTemplate") ReplyingSolaceTemplate solace,
            PersonFactory personFactory,
            InventoryCheckFactory inventoryCheckFactory) {
        this.bookingRequestService = bookingRequestService;
        this.quoteRequestService = quoteRequestService;
        this.inventoryRequestService = inventoryRequestService;
        this.solace = solace;
        this.personFactory = personFactory;
        this.inventoryCheckFactory = inventoryCheckFactory;
    }

    /**
     * The destinations this instance receives its replies on.
     *
     * <p>Each ends with this pod's id, and they are the first thing to check when replies do not
     * arrive. The booking and quote services share the first; inventory has its own.</p>
     *
     * @return the reply destination of each template, keyed by name
     */
    @GetMapping("/reply-destination")
    public Mono<Map<String, String>> replyDestination() {
        Map<String, String> destinations = new LinkedHashMap<>();
        destinations.put("shared", solace.getReplyDestination());
        destinations.put("inventory", inventoryRequestService.getReplyDestination());
        return Mono.just(destinations);
    }

    /**
     * One request-reply exchange.
     *
     * @return the reply payload with its send time, receive time and latency
     */
    @GetMapping({"/send", "/booking/send"})
    public Mono<ReplyResult<Person>> send() {
        return await(bookingRequestService.send(personFactory.create()));
    }

    /**
     * Several request-reply exchanges as independent publishes.
     *
     * <p>Requests go out immediately and the replies are streamed back as they arrive, so the
     * results are ordered by latency rather than by request.</p>
     *
     * @param count       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each reply as it arrives
     */
    @GetMapping({"/send-multiple", "/booking/send-multiple"})
    public Flux<ReplyResult<Person>> sendMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(personFactory.create(count))
                .flatMap(person -> await(bookingRequestService.send(person))
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several request-reply exchanges whose requests are published in one Solace transaction.
     *
     * <p>The requests reach the broker only when the transaction commits, so the futures are
     * awaited after the transactional call has returned &mdash; waiting inside it would block on
     * requests that have not been published yet.</p>
     *
     * @param count how many requests to publish in the transaction
     * @return every reply, once all of them have arrived
     */
    @GetMapping({"/send-batch", "/booking/send-batch"})
    public Mono<List<ReplyResult<Person>>> sendBatch(@RequestParam(defaultValue = "5") int count) {
        return Mono
                // the transaction opens and commits inside this call
                .fromCallable(() -> bookingRequestService.sendBatchInTransaction(personFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                // and only then is anything awaited
                .flatMap(futures -> Flux.fromIterable(futures).flatMap(this::await).collectList());
    }

    /**
     * High-concurrency benchmark: many exchanges streamed as they complete.
     *
     * @param total       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each reply as it arrives
     */
    @GetMapping("/benchmark")
    public Flux<ReplyResult<Person>> benchmark(
            @RequestParam(defaultValue = "100000") int total,
            @RequestParam(defaultValue = "1000") int concurrency) {
        return Flux.range(1, total)
                .map(i -> personFactory.create())
                .flatMap(person -> await(bookingRequestService.send(person))
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    // --- service two: quotes ------------------------------------------------------------

    /**
     * One quote exchange.
     *
     * @return the quote with its send time, receive time and latency
     */
    @GetMapping("/quote/send")
    public Mono<ReplyResult<Quote>> sendQuote() {
        return await(quoteRequestService.send(personFactory.create()));
    }

    /**
     * Several quote exchanges as independent publishes.
     *
     * @param count       how many exchanges to perform
     * @param concurrency how many to keep in flight at once
     * @return each quote as it arrives
     */
    @GetMapping("/quote/send-multiple")
    public Flux<ReplyResult<Quote>> sendQuoteMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(personFactory.create(count))
                .flatMap(person -> await(quoteRequestService.send(person))
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several quote requests published in one Solace transaction.
     *
     * @param count how many requests to publish in the transaction
     * @return every quote, once all of them have arrived
     */
    @GetMapping("/quote/send-batch")
    public Mono<List<ReplyResult<Quote>>> sendQuoteBatch(@RequestParam(defaultValue = "5") int count) {
        return Mono
                .fromCallable(() -> quoteRequestService.sendBatchInTransaction(personFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(futures -> Flux.fromIterable(futures).flatMap(this::await).collectList());
    }

    // --- service three: inventory, on its own reply destination ---------------------------

    /**
     * One inventory check.
     *
     * @return the inventory status with its send time, receive time and latency
     */
    @GetMapping("/inventory/send")
    public Mono<ReplyResult<InventoryStatus>> sendInventory() {
        return await(inventoryRequestService.send(inventoryCheckFactory.create()));
    }

    /**
     * Several inventory checks as independent publishes.
     *
     * @param count       how many checks to perform
     * @param concurrency how many to keep in flight at once
     * @return each status as it arrives
     */
    @GetMapping("/inventory/send-multiple")
    public Flux<ReplyResult<InventoryStatus>> sendInventoryMultiple(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "10") int concurrency) {
        return Flux.fromIterable(inventoryCheckFactory.create(count))
                .flatMap(check -> await(inventoryRequestService.send(check))
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    /**
     * Several inventory checks published in one Solace transaction.
     *
     * @param count how many checks to publish in the transaction
     * @return every status, once all of them have arrived
     */
    @GetMapping("/inventory/send-batch")
    public Mono<List<ReplyResult<InventoryStatus>>> sendInventoryBatch(
            @RequestParam(defaultValue = "5") int count) {
        return Mono
                .fromCallable(() -> inventoryRequestService
                        .sendBatchInTransaction(inventoryCheckFactory.create(count)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(futures -> Flux.fromIterable(futures).flatMap(this::await).collectList());
    }

    /**
     * Await one reply and pair it with the timings recorded on its future.
     *
     * @param future the outstanding request
     * @param <T>    the reply type
     * @return the reply with its timings
     */
    private <T> Mono<ReplyResult<T>> await(RequestReplyFuture<T> future) {
        return Mono.fromFuture(future)
                .log()
                .map(reply -> new ReplyResult<>(reply, future.getSendTime(), future.getReceiveTime()));
    }
}
