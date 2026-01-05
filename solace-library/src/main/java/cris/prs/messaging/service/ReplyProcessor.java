package cris.prs.messaging.service;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.SolaceRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;
import java.util.function.Consumer;

@Slf4j
@Service
@SuppressWarnings("unchecked")
public class ReplyProcessor {

    @Autowired
    RequestMapBean<Object> requestMapBean;

    public <T> Consumer<Message<T>> processReply(){
        return msg -> {
            MessageHeaders headers = msg.getHeaders();
            final String correlationId = headers.get(SolaceHeaders.CORRELATION_ID,String.class);
            T payload = msg.getPayload();

            final SolaceRequest<T> pending = (SolaceRequest<T>) requestMapBean.remove(correlationId);

            if (pending != null) {
                final long receiveTime = System.currentTimeMillis();
                ReplyResult<T> result = new ReplyResult<>(payload, pending.getSendTime(), receiveTime);
                pending.getFuture().complete(result);
            } else {
                log.warn("No outstanding request for CorrelationId={} with payload={}", correlationId, payload);
            }
        };
    }
}
