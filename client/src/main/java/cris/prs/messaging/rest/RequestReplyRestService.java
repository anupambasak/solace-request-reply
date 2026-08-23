package cris.prs.messaging.rest;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.service.BookingRequestService;
import cris.prs.messaging.service.PersonFactory;
import cris.prs.messaging.service.QuoteRequestService;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

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
 *   <li>{@code /request-reply/booking/*} &mdash; service one, replies with a {@code Person}</li>
 *   <li>{@code /request-reply/quote/*} &mdash; service two, replies with a {@code Quote}</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/request-reply")
@RequiredArgsConstructor
public class RequestReplyRestService {

    private final BookingRequestService bookingRequestService;

    private final QuoteRequestService quoteRequestService;

    private final ReplyingSolaceTemplate solace;

    private final PersonFactory personFactory;

    /**
     * The destination this instance receives its replies on.
     *
     * <p>Ends with this pod's id, and is the first thing to check when replies do not arrive.</p>
     *
     * @return the reply destination
     */
    @GetMapping("/reply-destination")
    public Mono<String> replyDestination() {
        return Mono.just(solace.getReplyDestination());
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
