package cris.prs.messaging.rest;

import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.RequestReplyService;
import lombok.extern.slf4j.Slf4j;
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

    @Value("${replyTopic}")
    private String replyTopic;

    @Autowired
    private RequestReplyService rrs;

    @GetMapping("/test")
    public Mono<String> test(){
        return Mono.just("OK Hello World");
    }

    @GetMapping("/send")
    public Mono<ReplyResult<?>> ss(@RequestParam("cmd") String cmd){
        Message<String> msg = MessageBuilder.withPayload(cmd)
                .build();
        final String topic = "bkg/trn";
        CompletableFuture<ReplyResult<?>> cc = rrs.sendAndReceive(topic, msg, replyTopic);
        return Mono.fromFuture(cc).map(ss -> {
            log.info("{}",new String((byte[]) ss.getPayload()));
            return ss;
        });
    }

    @GetMapping("/send-bulk-stream")
    public Flux<ReplyResult<?>> sendBulkStream() {
        int total = 100_000;
        int concurrency = 1000;

        final Message<String> msg = MessageBuilder.withPayload("asdf")
                .build();

        final String topic = "bkg/trn";
        return Flux.range(1, total)
                .flatMap(i -> Mono.fromFuture(rrs.sendAndReceive(topic, msg, replyTopic))
                                .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }
}
