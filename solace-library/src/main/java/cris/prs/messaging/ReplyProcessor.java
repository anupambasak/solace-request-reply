package cris.prs.messaging;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;
import java.util.function.Consumer;

@Slf4j
@Service
public class ReplyProcessor {

    @Autowired
    RequestMapBean requestMapBean;

    public <T> Consumer<Message<T>> processReply(){
        return msg -> {
            MessageHeaders headers = msg.getHeaders();
            final String correlationId = headers.get(SolaceHeaders.CORRELATION_ID,String.class);
            T payload = msg.getPayload();

            PendingRequest pending = requestMapBean.remove(correlationId);

            if (pending != null) {
                long receiveTime = System.currentTimeMillis();
                ReplyResult<T> result = new ReplyResult<>(payload, pending.sendTime, receiveTime);
                pending.future.complete(result);
            } else {
                log.warn("No outstanding request for CorrelationId={} with payload={}", correlationId, payload);
            }
        };
    }
}
