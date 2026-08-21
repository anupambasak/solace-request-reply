package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Map;

/**
 * A received message together with its converted payload &mdash; the Solace analogue of Spring for
 * Apache Kafka's {@code ConsumerRecord}.
 *
 * @param <T> the converted payload type
 */
@Getter
@ToString(exclude = "rawMessage")
@RequiredArgsConstructor
public class SolaceRecord<T> {

    private final T payload;
    private final String destination;
    private final String correlationId;
    private final String replyTo;
    private final Map<String, Object> headers;
    private final BytesXMLMessage rawMessage;

    public boolean isRedelivered() {
        return this.rawMessage != null && this.rawMessage.getRedelivered();
    }
}
