package cris.prs.messaging;

import java.util.concurrent.CompletableFuture;

public class PendingRequest {
    final long sendTime;
    final CompletableFuture<ReplyResult> future;

    PendingRequest(long sendTime, CompletableFuture<ReplyResult> future) {
        this.sendTime = sendTime;
        this.future = future;
    }
}
