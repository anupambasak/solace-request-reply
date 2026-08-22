package cris.prs.messaging.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/** Liveness check, kept apart from the pattern controllers. */
@RestController
public class HealthRestService {

    /** @return a fixed greeting, confirming the web layer is up */
    @GetMapping("/test")
    public Mono<String> test() {
        return Mono.just("OK Hello World");
    }
}
