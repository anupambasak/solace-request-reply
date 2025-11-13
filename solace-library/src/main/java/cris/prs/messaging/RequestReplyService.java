package cris.prs.messaging;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class RequestReplyService {

    @Autowired
    RequestMapBean requestMapBean;

    @Autowired
    private StreamBridge sb;

    @Value("${HOSTNAME}")
    private String currentHost;

    public <T> CompletableFuture<ReplyResult<?>> sendAndReceive(String requestTopic, Message<T> payload, String replyTopic){

        final String correlationId = UUID.randomUUID().toString();
        Destination topic = JCSMPFactory.onlyInstance().createTopic(replyTopic);

        CompletableFuture<ReplyResult<?>> future = new CompletableFuture<>();

        long sendTime = System.currentTimeMillis();
        requestMapBean.put(correlationId, new PendingRequest(sendTime, future));

        Message<T> msg = MessageBuilder.fromMessage(payload)
                .setHeader(SolaceHeaders.CORRELATION_ID,correlationId)
                .setHeader("hostname", currentHost)
                .setHeader(SolaceHeaders.REPLY_TO, topic)
                .build();

        try {
            sb.send(requestTopic, msg);
            log.info("Sent message with CorrelationId={} at {}", correlationId, Instant.ofEpochMilli(sendTime));
        } catch (Exception e) {
            requestMapBean.remove(correlationId);
            future.completeExceptionally(e);
            log.error("Failed to send message with CorrelationId={}", correlationId, e);
        }

        return future;

    }

    public <T> Message<T> sendReplyToMessage(MessageHeaders headers, T payload){

        String correlationId = headers.get(SolaceHeaders.CORRELATION_ID,String.class);
        String hostName = headers.get("hostname",String.class);
        Topic topic = headers.get(SolaceHeaders.REPLY_TO,Topic.class);
        if(log.isTraceEnabled()) {
            log.trace("Headers:{}", headers);
            log.trace("Consuming Message {}:{}", SolaceHeaders.CORRELATION_ID, correlationId);
        }
        assert topic != null : "topic is null";
        return  MessageBuilder.withPayload(payload)
                .setHeader(SolaceHeaders.CORRELATION_ID,correlationId)
                .setHeader("hostname",hostName)
                .setHeader(BinderHeaders.TARGET_DESTINATION,topic.getName())
                .build();
    }
}
