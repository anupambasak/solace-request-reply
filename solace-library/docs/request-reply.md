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

## Additional reply destinations

One reply destination per application instance, shared by every service it calls, is the right
default: the reply channel belongs to the **requester**, the correlation id returns each reply to its
request, and the cost is one endpoint per pod rather than one per pod per service.

Declare a second template from `ReplyingSolaceTemplateFactory` when sharing stops paying:

```java
@Bean
ReplyingSolaceTemplate inventoryReplyingSolaceTemplate(ReplyingSolaceTemplateFactory factory) {
    ReplyEndpointSpec spec = new ReplyEndpointSpec();
    spec.setId("inventoryReplyContainer");          // unique container id
    spec.setReplyTopicPrefix("app/reply/inventory");
    spec.setConcurrency(2);
    return factory.create(spec);
}
```

The returned template is a `SmartLifecycle` bean, so Spring starts and stops it, and it owns its
container. Inject it with `@Qualifier`, since more than one `ReplyingSolaceTemplate` now exists.

### When a separate destination is worth the extra endpoint

| | |
| :--- | :--- |
| **Head-of-line blocking** | A high-volume service's replies delay a latency-sensitive one's on the shared flow. Raising `solace.request-reply.concurrency` is the cheaper fix (it needs a durable reply queue); a separate endpoint is the thorough one. |
| **Different trust domains** | Anything able to publish to a shared reply topic could forge a reply for another service, given a guessed correlation id. Topic ACLs cannot separate them. |
| **Blast radius** | A stalled or full reply endpoint stops every conversation sharing it. |
| **Observability** | Queue depth and latency per service rather than in aggregate. |

Two or three services in one trust domain, at similar volumes, do **not** justify it — the
correlation id already keeps their conversations apart, including when they reply with different
types.

### Nothing changes on the responder

A responder never names a reply destination: it returns a value and the container publishes it to
the request's `replyTo`. Which destination that is was decided by the requesting client. Splitting a
service onto its own reply destination therefore requires no change to the service answering it.

### ReplyEndpointSpec

| Property | Default | Description |
| :--- | :--- | :--- |
| `id` | `solaceReplyContainer` | Container id; must be unique when more than one reply destination is in use. |
| `replyTopicPrefix` | `reply` | Replies arrive on `<prefix>/<instance-id>`. |
| `appendInstanceId` | `true` | Off makes every instance share one destination, so replies reach the wrong requester. |
| `endpointMode` | `NON_DURABLE_QUEUE` | How the reply endpoint binds. |
| `replyQueue` | derived from the prefix | Base endpoint name. |
| `replyGroup` | `null` | Group segment for a shared durable reply endpoint. |
| `selector` | `null` | Broker-side selector. |
| `concurrency` | `1` | Above one the endpoint is provisioned non-exclusive. Only meaningful with `endpointMode: DURABLE_QUEUE` — a non-durable reply endpoint is a temporary queue and accepts exactly one flow, so a higher value is clamped to 1 with a warning. |
| `replyTimeout` | `30s` | Before the future fails. |
| `deliveryMode` | `PERSISTENT` | For requests published through the template. |

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
