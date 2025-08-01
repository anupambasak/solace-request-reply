package cris.prs.msg;

import java.time.Instant;

public class ReplyResult {
    private final String payload;
    private final long sendTime;
    private final long receiveTime;
    private final long latency;

    public ReplyResult(String payload, long sendTime, long receiveTime) {
        this.payload = payload;
        this.sendTime = sendTime;
        this.receiveTime = receiveTime;
        this.latency = receiveTime - sendTime;
    }

    public String getPayload() { return payload; }
    public long getSendTime() { return sendTime; }
    public long getReceiveTime() { return receiveTime; }
    public long getLatency() { return latency; }

    @Override
    public String toString() {
        return String.format("Payload=%s, Sent=%s, Received=%s, Latency=%dms",
                payload, Instant.ofEpochMilli(sendTime), Instant.ofEpochMilli(receiveTime), latency);
    }
}
