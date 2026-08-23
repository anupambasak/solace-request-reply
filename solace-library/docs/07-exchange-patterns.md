# 7. Exchange patterns

`ExchangePattern` is the one attribute that turns a handful of low-level endpoint decisions into a
single word. Setting `pattern` fills in `endpointMode`, `appendInstanceIdToQueue`, `accessType` and
sometimes `concurrency` — and nothing else. Everything it sets can be overridden.

---

## 7.1 The one idea behind all three

In Solace, a topic is a *routing key on a published message*, not a place. Consumers bind to an
**endpoint** (a queue) and attach topic subscriptions to it. So a publisher does exactly the same
thing in all three patterns — publishes to a topic — and the entire difference between broadcast,
work-sharing and request-reply is **how many endpoints exist and who binds to them**.

```
                              ┌─────────────────────┐
     publish "orders/created" │      Solace broker  │
     ───────────────────────► │                     │
                              │  matches every      │
                              │  subscription       │
                              └──────────┬──────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        │ PUBLISH_SUBSCRIBE              │ POINT_TO_POINT                 │
        │ one endpoint PER INSTANCE      │ ONE endpoint, shared           │
        ▼                                ▼                                │
  audit.pod-a  audit.pod-b  audit.pod-c        tasks.workers              │
      │            │            │              │    │    │                │
    pod-a        pod-b        pod-c          pod-a pod-b pod-c            │
   (a copy)     (a copy)     (a copy)         (one of them gets it)       │
```

That is the whole distinction. `appendInstanceIdToQueue` is the switch.

---

## 7.2 What each pattern sets

`SolaceListenerEndpoint.applyPatternDefaults()` runs **after** the annotation attributes are read and
only writes into fields still unset:

| | `endpointMode` | `appendInstanceIdToQueue` | `accessType` | `concurrency` |
| :--- | :--- | :--- | :--- | :--- |
| `PUBLISH_SUBSCRIBE` | `NON_DURABLE_QUEUE` | `true` | `EXCLUSIVE` | `1` |
| `POINT_TO_POINT` | `DURABLE_QUEUE` | `false` | `NONEXCLUSIVE` | *(inherited)* |
| `REQUEST_REPLY` | *(inherited)* | `false` | `NONEXCLUSIVE` | *(inherited)* |

Blank means "left to `solace.listener.*`".

---

## 7.3 `PUBLISH_SUBSCRIBE` — every instance gets a copy

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "config", topics = "config/changed")
public void onConfigChange(ConfigChange change) { … }
```

Endpoint: `config.<instance-id>` — temporary, exclusive, one flow.

Each instance provisions its **own** temporary queue and subscribes it to the same topic. The broker
spools a copy into each, so every instance sees every message. When the pod dies its queue goes with
it, leaving nothing behind to accumulate — which is exactly what you want for cache invalidation,
configuration refresh, or a local index update.

**Why `concurrency` is pinned to 1.** The endpoint is exclusive *and* temporary. An exclusive
endpoint admits one active consumer, and a temporary endpoint rejects surplus binds outright with
`503 Max clients exceeded for queue`. Without the pin, a listener would silently inherit
`solace.listener.concurrency: 10` and fail at startup. Parallelism in fan-out comes from running
more instances — which is the point of the pattern.

**Trade-off to know:** because the queue is temporary, messages published while an instance is down
are not held for it. Use `POINT_TO_POINT` (or a durable per-instance queue) if a restarting instance
must catch up.

## 7.4 `POINT_TO_POINT` — exactly one consumer

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "tasks", group = "workers",
        topics = "tasks/submit", concurrency = "10")
public void onTask(Task task) { … }
```

Endpoint: `tasks.workers` — durable, non-exclusive, ten flows per instance.

Every instance binds to the **same** queue, so the broker shares messages out across all flows on it.
Scaling the deployment adds flows to the same endpoint; it does not duplicate delivery.

Because the queue is durable it survives restarts with its messages and its subscriptions, so work
published while every consumer is down is waiting when they come back. That durability is also why
`400 Subscription Already Exists` is the *normal* second-start response, and why the container
tolerates it.

**Consumer groups.** `queue` + `group` is a naming convention, not a broker protocol: `tasks.workers`
and `tasks.audit` subscribed to the same topic are two independent groups, each getting a full copy
of the stream, each sharing internally. That is how you get Kafka-style consumer groups here.

**Exclusive variant.** Set `accessType = EXCLUSIVE` (via `solace.listener.endpoint.access-type`) for
strict ordering: one active consumer, the others hot standby, automatic failover. Then keep
`concurrency: 1`.

## 7.5 `REQUEST_REPLY` — a response comes back

```java
@SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", group = "v1",
        topics = "pricing/quote", concurrency = "5", transactional = "true")
public Quote quote(PriceRequest request) {
    return new Quote(request.sku(), price(request));
}
```

Endpoint: `pricing.v1` — non-exclusive, shared. Structurally this is `POINT_TO_POINT` with one
addition: **the return value is published back to the request's `replyTo`**.

`endpointMode` is deliberately *not* set by this pattern. A request endpoint is usually durable, but
a genuinely fire-and-forget RPC over a temporary endpoint is legitimate, so the choice is left to
configuration.

The responder never decides where the reply goes. It echoes the `replyTo` the requester stamped on
the request. That keeps the reply channel owned by the party that needs it, and it is why
`replyDestination` should stay empty on almost every listener. See
[10. Request-reply](10-request-reply.md) for the requester side.

---

## 7.6 Independence rules for multiple services

Adding a second request-reply service is where the topic/endpoint distinction bites hardest.

**A second service needs its own request topic, not just its own queue.** Two queues subscribed to
the same topic each receive a *copy* of every request, so both services would answer — the requester
matches one reply by correlation id and logs the other as an orphan. The queue separates the
consumers; only a different topic separates the *messages*.

| Service | Request topic | Endpoint |
| :--- | :--- | :--- |
| booking | `request-reply/request-1` | `request-reply-queue-1.request-reply-group-1` |
| quote | `request-reply/request-2` | `request-reply-queue-2.request-reply-group-2` |
| inventory | `request-reply/request-3` | `request-reply-queue-3.request-reply-group-3` |

Reply destinations are the opposite case: services *should* share one per-instance reply destination
by default, because the reply channel belongs to the requester and the correlation id keeps
conversations apart. One endpoint per pod beats pods × services. See
[10.6](10-request-reply.md#106-when-to-split-a-reply-destination) for the four conditions that
justify splitting one out.

---

## 7.7 Choosing

| You want | Pattern | Key detail |
| :--- | :--- | :--- |
| Cache invalidation, config refresh, local index update | `PUBLISH_SUBSCRIBE` | Temporary per-instance queue; nothing accumulates |
| Work queue, job processing, load-shared commands | `POINT_TO_POINT` | Durable shared queue; scale by adding instances |
| Synchronous-feeling RPC over messaging | `REQUEST_REPLY` | Return a value; requester owns the reply channel |
| Broadcast that survives a restart | `POINT_TO_POINT`, one group per consumer | Durable per-group queues, each getting a full copy |
| Address one specific instance | `PUBLISH_SUBSCRIBE` + `appendInstanceIdToTopics` | Subscribes `topic/<instance-id>` |

---

**Next:** [8. Producing messages](08-producing-messages.md)
