package cris.prs.messaging.consumer;

import cris.prs.messaging.RequestReplyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Slf4j
@Component
public class ServiceConsumer {

    @Autowired
    private RequestReplyService rrs;

    @Bean
    public Function<Message<String>, Message<String>> booking(){
        return msg -> {
            String v = msg.getPayload();
            log.info("Payload: {}",v);
            if("sleep".equals(v)){
                try {
                    log.info("Going to sleep for 10s");
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    log.error("<Error>",e);
                }
            }
            v = v.toUpperCase(); // processing finished
            return rrs.sendReplyToMessage(msg.getHeaders(), v);
        };
    }
}
