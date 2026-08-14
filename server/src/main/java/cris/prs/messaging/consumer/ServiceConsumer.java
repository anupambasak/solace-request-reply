package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.service.RequestReplyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Slf4j
@Component
public class ServiceConsumer {

    @Autowired
    private RequestReplyService rrs;

    @Bean
    public Function<Message<Person>, Message<Person>> booking(){
        return msg -> {
            MessageHeaders headers = msg.getHeaders();
            headers.forEach((k,v) -> log.info("Header {}:{}",k,v));
            Person p = msg.getPayload();
            log.info("Payload: {}",p);
            p.setName(p.getName().toUpperCase());
            p.setAge(p.getAge()+23);
            return rrs.sendReplyToMessage(msg.getHeaders(), p);
        };
    }
}
