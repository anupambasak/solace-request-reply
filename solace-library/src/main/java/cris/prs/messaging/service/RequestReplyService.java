package cris.prs.messaging.service;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;
import cris.prs.messaging.ReplyResult;
import cris.prs.messaging.SolaceRequest;
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
@SuppressWarnings({"unchecked","rawtypes"})
public class RequestReplyService {

    @Autowired
    RequestMapBean requestMapBean;

    @Autowired
    private StreamBridge sb;

    @Value("${HOSTNAME}")
    private String currentHost;

    public <T,R> CompletableFuture<ReplyResult<R>> sendAndReceive(String requestTopic, T payload, String replyTopic, Class<R> responseClass){

        final String correlationId = UUID.randomUUID().toString();
        final Destination topic = JCSMPFactory.onlyInstance().createTopic(replyTopic);
        final CompletableFuture<ReplyResult<R>> future = new CompletableFuture<>();

        try {
            final long sendTime = System.currentTimeMillis();
            requestMapBean.put(correlationId, new SolaceRequest<R>(sendTime, future));

            final Message<T> msg = MessageBuilder.withPayload(payload)
                    .setHeader(SolaceHeaders.CORRELATION_ID,correlationId)
                    .setHeader("hostname", currentHost)
                    .setHeader(SolaceHeaders.REPLY_TO, topic)
                    .build();
            sb.send(requestTopic, msg);
            log.debug("Sent message with CorrelationId={} at {} to request-topic {}", correlationId, Instant.ofEpochMilli(sendTime),requestTopic);
        } catch (Exception e) {
            future.completeExceptionally(e);
            log.error("Failed to sendind message with correlationId={}", correlationId, e);
        } finally {
            requestMapBean.remove(correlationId);
        }
        return future;
    }

    public <R> Message<R> sendReplyToMessage(MessageHeaders headers, R payload){

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
