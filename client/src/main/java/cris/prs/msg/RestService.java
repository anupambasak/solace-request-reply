package cris.prs.msg;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private RequestReplyService rrs;

    @GetMapping("/test")
    public Mono<String> test(){
        return Mono.just("OK Hello World");
    }

    @GetMapping("/send")
    public Mono<ReplyResult> ss(@RequestParam("cmd") String cmd){
        CompletableFuture<ReplyResult> cc = rrs.sendAndReceive(cmd);
        return Mono.fromFuture(cc);
    }

    @GetMapping("/send-bulk-stream")
    public Flux<ReplyResult> sendBulkStream() {
        int total = 100_000;
        int concurrency = 1000;

        return Flux.range(1, total)
                .flatMap(i -> Mono.fromFuture(rrs.sendAndReceive("asdf"))
                                .subscribeOn(Schedulers.boundedElastic()), concurrency);
    }
}
