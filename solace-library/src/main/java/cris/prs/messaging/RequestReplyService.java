package cris.prs.messaging;

import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
@Service
public class RequestReplyService {

    private final Map<String, PendingRequest> outstandingRequests = new ConcurrentHashMap<>();

    @Autowired
    private StreamBridge sb;

    @Value("${replyTopic}")
    private String replyTopic;

    @Value("${HOSTNAME}")
    private String currentHost;

    public <T> CompletableFuture<ReplyResult> sendAndReceive(Message<T> payload){

        String correlationId = UUID.randomUUID().toString();
        Destination topic = JCSMPFactory.onlyInstance().createTopic(replyTopic);

        CompletableFuture<ReplyResult> future = new CompletableFuture<>();

        long sendTime = System.currentTimeMillis();
        outstandingRequests.put(correlationId, new PendingRequest(sendTime, future));

        Message<T> msg = MessageBuilder.fromMessage(payload)
                .setHeader(SolaceHeaders.CORRELATION_ID,correlationId)
                .setHeader("hostname", currentHost)
                .setHeader(SolaceHeaders.REPLY_TO, topic)
                .build();

        try {
            sb.send("bkg/trn", msg);
            log.info("Sent message with CorrelationId={} at {}", correlationId, Instant.ofEpochMilli(sendTime));
        } catch (Exception e) {
            outstandingRequests.remove(correlationId);
            future.completeExceptionally(e);
            log.error("Failed to send message with CorrelationId={}", correlationId, e);
        }

        return future;

    }

    @Bean
    public Consumer<Message<String>> bookingreply(){
        return msg -> {
            MessageHeaders headers = msg.getHeaders();
            String correlationId = headers.get(SolaceHeaders.CORRELATION_ID,String.class);
            String payload = msg.getPayload();

            PendingRequest pending = outstandingRequests.remove(correlationId);

            if (pending != null) {
                long receiveTime = System.currentTimeMillis();
                ReplyResult result = new ReplyResult(payload, pending.sendTime, receiveTime);
//                log.info("Received reply for CorrelationId={} at {} (latency={} ms), payload={}",
//                        correlationId, Instant.ofEpochMilli(receiveTime), result.getLatency(), payload);
                pending.future.complete(result);
            } else {
                log.warn("No outstanding request for CorrelationId={} with payload={}", correlationId, payload);
            }
        };
    }
}
