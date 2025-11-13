package cris.prs.messaging.reply.consumers;

import cris.prs.messaging.ReplyProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Slf4j
@Service
public class ReplyConsumer {

    @Autowired
    private ReplyProcessor replyProcessor;

    @Bean
    public <T> Consumer<Message<T>> myReplyConsumer(){
        return replyProcessor.processReply();
    }
}
