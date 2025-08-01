package cris.prs.msg;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solacesystems.jcsmp.Topic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Slf4j
@Component
public class MyConsumer {

    @Bean
    public Function<Message<String>, Message<String>> booking(){
        return msg -> {
            MessageHeaders headers = msg.getHeaders();
            String correlationId = headers.get(SolaceHeaders.CORRELATION_ID,String.class);
            String hostName = msg.getHeaders().get("hostname",String.class);
            Topic topic = headers.get(SolaceHeaders.REPLY_TO,Topic.class);
//            log.info("Headers:{}",headers);
//            log.info("Consuming Message {}:{}",SolaceHeaders.CORRELATION_ID,correlationId);
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
            return MessageBuilder.withPayload(v.toUpperCase())
                    .setHeader(SolaceHeaders.CORRELATION_ID,correlationId)
                    .setHeader("hostname",hostName)
                    .setHeader(BinderHeaders.TARGET_DESTINATION,topic.getName())
                    .build();
        };
    }

//    @Bean
//    public Function<String, String> booking(){
//        return String::toUpperCase;
//    }

//    @Bean
//    public Consumer<String> booking(){
//        return v -> log.info("Message: {}",v) ;
//    }

//    @Bean
//    public Consumer<Message<String>> booking(){
//        return v -> log.info("Message: {}",v) ;
//    }
}
