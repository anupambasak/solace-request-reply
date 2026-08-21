package cris.prs.messaging.rest;

import cris.prs.messaging.Person;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.service.BookingRequestService;
import cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Reactive front end for the request-reply demo.
 *
 * <p>Replies arrive on this pod's own reply topic ({@code <prefix>/<hostname>}), so several
 * replicas can serve requests concurrently without any broker side filtering.</p>
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class RestService {

    private final Faker faker = new Faker();

    private final ReplyingSolaceTemplate solace;

    private final BookingRequestService bookingRequestService;

    @Value("${app.request.topic:bkg/trn}")
    private String requestTopic;

    @GetMapping("/test")
    public Mono<String> test() {
        return Mono.just("OK Hello World");
    }

    /** The destination this instance receives its replies on; handy when scaled out. */
    @GetMapping("/reply-destination")
    public Mono<String> replyDestination() {
        return Mono.just(solace.getReplyDestination());
    }

    @GetMapping("/sendperson")
    public Mono<ReplyResult<Person>> sendperson() {
        RequestReplyFuture<Person> future = solace.sendAndReceive(this.requestTopic, randomPerson(), Person.class);
        return toResult(future);
    }

    /**
     * Same exchange, but the request is published inside a Solace local transaction driven by
     * {@code @Transactional}; the reply is awaited after the transaction has committed.
     */
    @GetMapping("/sendperson-tx")
    public Mono<ReplyResult<Person>> sendpersonTransactional() {
        RequestReplyFuture<Person> future =
                bookingRequestService.sendInTransaction(this.requestTopic, randomPerson());
        return toResult(future);
    }

    @GetMapping("/send-bulk-stream")
    public Flux<ReplyResult<Person>> sendBulkStream() {
        int total = 100_000;
        int concurrency = 1000;

        return Flux.range(1, total)
                .map(i -> randomPerson())
                .flatMap(person -> toResult(solace.sendAndReceive(this.requestTopic, person, Person.class))
                        .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }

    private Mono<ReplyResult<Person>> toResult(RequestReplyFuture<Person> future) {
        return Mono.fromFuture(future)
                .map(reply -> new ReplyResult<>(reply, future.getSendTime(), future.getReceiveTime()));
    }

    private Person randomPerson() {
        Person person = new Person();
        person.setName(faker.name().fullName());
        person.setAge(faker.number().numberBetween(18, 80));
        return person;
    }
}
