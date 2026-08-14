package cris.prs.messaging.rest;

import cris.prs.messaging.Person;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.service.RequestReplyService;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
public class RestService {

    private final Faker faker = new Faker();

    @Value("${replyTopic}")
    private String replyTopic;

    @Autowired
    private RequestReplyService rrs;

    @GetMapping("/test")
    public Mono<String> test(){
        return Mono.just("OK Hello World");
    }

    @GetMapping("/sendperson")
    public Mono<ReplyResult<Person>> sendperson(){
        Person p = new Person();
        p.setName(faker.name().fullName());
        p.setAge(faker.number().numberBetween(18, 80));
        final String topic = "bkg/trn";
        CompletableFuture<ReplyResult<Person>> cc = rrs.sendAndReceive(topic, p, replyTopic, Person.class);
        return Mono.fromFuture(cc).map(ss -> {
            Person pp = ss.getPayload();
            log.info("pp:  {}", pp);
            return ss;
        });
    }

    @GetMapping("/send-bulk-stream")
    public Flux<ReplyResult<Person>> sendBulkStream() {
        int total = 100_000;
        int concurrency = 1000;

        final String topic = "bkg/trn";
        return Flux.range(1, total)
                .map(i -> {
                    Person p = new Person();
                    p.setName(faker.name().fullName());
                    p.setAge(faker.number().numberBetween(18, 80));
                    return p;
                })
                .flatMap(p -> Mono.fromFuture(rrs.sendAndReceive(topic, p, replyTopic, Person.class))
                                .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }
}
