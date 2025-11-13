package cris.prs.messaging;

import lombok.Getter;

import java.time.Instant;

@Getter
public class ReplyResult<T> {
    private final T payload;
    private final long sendTime;
    private final long receiveTime;
    private final long latency;

    public ReplyResult(T payload, long sendTime, long receiveTime) {
        this.payload = payload;
        this.sendTime = sendTime;
        this.receiveTime = receiveTime;
        this.latency = receiveTime - sendTime;
    }

    @Override
    public String toString() {
        return String.format("Payload=%s, Sent=%s, Received=%s, Latency=%dms",
                payload.toString(), Instant.ofEpochMilli(sendTime), Instant.ofEpochMilli(receiveTime), latency);
    }
}
