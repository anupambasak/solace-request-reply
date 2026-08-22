# `requestreply` — asynchronous request-reply

Package `cris.prs.messaging.solace.requestreply`.

---

## ReplyingSolaceTemplate

The counterpart of `ReplyingKafkaTemplate`. Extends `SolaceTemplate<Object>`, so every publishing
method is available too, and implements `SmartLifecycle`, `InitializingBean` and `DisposableBean`.

Each request is stamped with a correlation id and a `replyTo` destination unique to this instance —
the reply topic carries the pod or host name as its last level. A horizontally scaled deployment
therefore needs neither broker-side selectors nor a shared reply queue: a reply can only reach the
instance that asked for it.

**Constructor:** `(SolaceSessionFactory, SolaceMessageConverter, SolaceMessageListenerContainer
replyContainer, String replyDestination)`

| Property | Default | Description |
| :--- | :--- | :--- |
| `defaultReplyTimeout` | `30s` | Applied when a call does not pass one. Zero or negative disables the timeout, and the future then waits indefinitely. |
| `instanceId` | `null` | Sent as the `instanceId` SDT property on every request. |
| `autoStartup` | `true` | |
| `phase` | `Integer.MAX_VALUE - 90` | Later than the containers, so replies cannot arrive before the correlation map is live. |

| Method | Description |
| :--- | :--- |
| `sendAndReceive(String destination, Object payload, Class<T> replyType)` | Publish a request; the future completes with the reply converted to `replyType`. Uses `defaultReplyTimeout`. |
| `sendAndReceive(..., Class<T> replyType, Duration replyTimeout)` | As above with an explicit timeout. |
| `sendAndReceive(..., Map<String,Object> headers, Class<T> replyType, Duration replyTimeout)` | Full form. Extra headers become SDT user properties. |
| `getReplyDestination()` | The destination this instance receives replies on — worth exposing over HTTP when debugging a scaled deployment. |
| `getPendingCount()` | Requests still awaiting a reply; useful as a metric and in tests. |
| `onReply(BytesXMLMessage)` *(protected)* | The reply listener. Matches the correlation id and completes the waiting future. Override to add tracing. |
| `afterPropertiesSet()` | Registers `onReply` with the reply container. |
| `start()` / `stop()` / `isRunning()` / `isAutoStartup()` / `getPhase()` / `destroy()` | Lifecycle. `stop()` fails every outstanding future rather than leaving callers blocked. |

Every request carries: `solace_correlationId`, `solace_replyTo`, `requestSendTime`, and `instanceId`
when set.

### Timeouts

Each request schedules a timeout task on a single daemon thread (`solace-reply-timeout`). The task is
cancelled when the future completes, so a healthy exchange leaves nothing behind. On expiry the
correlation entry is removed and the future fails with
[`SolaceReplyTimeoutException`](#solacereplytimeoutexception) — the entry is removed first, so a late
reply is reported as unmatched rather than completing an already-failed future.

### Transactions

When a Solace transaction is active on the calling thread the request is only released at commit, so
the future must be awaited **outside** the transaction:

```java
@Transactional
public RequestReplyFuture<Ack> send(Order order) {
    return solace.sendAndReceive("orders/place", order, Ack.class);   // published at commit
}

// caller, after the transactional method returns:
Ack ack = send(order).get();
```

### Usage

```java
@Autowired ReplyingSolaceTemplate solace;

RequestReplyFuture<Ack> future = solace.sendAndReceive("orders/place", order, Ack.class);
Ack ack = future.get();
long latency = future.getLatency();
```

Reactively:

```java
return Mono.fromFuture(future)
           .map(ack -> new Result(ack, future.getSendTime(), future.getReceiveTime()));
```

---

## RequestReplyFuture&lt;R&gt;

`CompletableFuture<R>` carrying the correlation id and the timings needed to report round-trip
latency.

| Accessor | Description |
| :--- | :--- |
| `getCorrelationId()` | Correlation id sent with the request. |
| `getSendTime()` | Millisecond epoch at which the request was published. |
| `getReceiveTime()` | Millisecond epoch at which the reply arrived; `0` until then. |
| `getRequestDestination()` | Topic the request went to. |
| `getReplyDestination()` | Destination the reply was expected on. |
| `getLatency()` | `receiveTime - sendTime`. Meaningful only once the future has completed with a reply. |

## SolaceReplyTimeoutException

Extends `SolaceMessagingException`. Raised into the future when no reply arrives in time, and into
every outstanding future when the template stops.

---

## The serving side

There is no special API: an ordinary `@SolaceListener` whose method returns a value. The container
publishes that value to the destination in the request's `replyTo`, copying the correlation id.

```java
@SolaceListener(pattern = "REQUEST_REPLY", topics = "orders/place", queue = "orders", group = "svc")
public Ack place(Order order) {
    return acknowledge(order);
}
```

Returning `void` makes it a plain consumer. With `transactional = "true"` the acknowledgement of the
request and the publication of the reply commit together — see [Transactions](transactions.md).
