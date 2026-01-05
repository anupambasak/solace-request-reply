package cris.prs.messaging;

import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class SolaceRequest<V> {
    private final long sendTime;
    private final CompletableFuture<ReplyResult<V>> future;

    public SolaceRequest(long sendTime, CompletableFuture<ReplyResult<V>> future) {
        this.sendTime = sendTime;
        this.future = future;
    }
}
