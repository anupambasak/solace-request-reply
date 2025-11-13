package cris.prs.messaging.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.reactive.config.EnableWebFlux;

@SpringBootApplication
@EnableWebFlux
@ComponentScan(basePackages = {"cris.prs.messaging"})
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}