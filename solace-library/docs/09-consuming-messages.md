# 9. Consuming messages

The `listener` package: how an annotated method becomes a running consumer, and everything that
happens to a message between the broker and that method.

---

## 9.1 The pieces

| Type | Role |
| :--- | :--- |
| `SolaceListenerEndpoint` | A value object describing *what* to consume: topics, queue, group, pattern, and every override. Produced from `@SolaceListener`, or built by hand. |
| `SolaceListenerContainerFactory` | Turns an endpoint into a container. One method: `createListenerContainer(endpoint)`. |
| `DefaultSolaceListenerContainerFactory` | The implementation. Holds the shared collaborators and the default `ContainerProperties`. |
| `SolaceMessageListenerContainer` | `SmartLifecycle` + `getListenerId()` + `setupMessageListener(...)`. |
| `DefaultSolaceMessageListenerContainer` | The implementation: provisioning, flows, dispatch, transactions, shutdown. |
| `SolaceListenerEndpointRegistry` | Holds every container by id and lifecycles them as one bean. |
| `SolaceMessageListener` | `void onMessage(BytesXMLMessage)` — the low-level callback. |
| `AbstractSolaceListenerAdapter` | Conversion, `SolaceRecord` construction, and reply publishing, shared by both adapters. |
| `MethodSolaceListenerAdapter` | Invokes an `InvocableHandlerMethod` — what `@SolaceListener` uses. |
| `RecordSolaceListenerAdapter<T,R>` | Invokes a `Function<SolaceRecord<T>, R>` — for programmatic registration. |
| `SolaceListenerErrorHandler` | `void handleError(BytesXMLMessage, Exception)`, plus an optional `resolveOutcome` for per-failure settlement. Defaults to logging. |
| `SettlementOutcome` | `ACCEPTED` / `FAILED` / `REJECTED` / `NONE` — what happens to a message whose listener threw. |
| `SolaceFlowListener` | `void onFlowEvent(SolaceFlowEventArgs)` — flow lifecycle callback. |
| `SolaceFlowEvent` | `UP` / `DOWN` / `RECONNECTING` / `RECONNECTED` / `ACTIVE` / `INACTIVE` / `UNKNOWN`. |
| `TopicDispatchingSolaceListener` | Routes a shared endpoint's messages to the method whose subscription matched. |
| `TopicDispatchTarget` | One method in a dispatch group: its topics, handler and payload type. |
| `ReplayStartPoint` | `BEGINNING`, or an instant. Where a replay starts. |
| `ContainerProperties` | Every container setting; also the type `solace.listener.*` binds to. |
| `ContainerKeepAlive` | Package-private, reference-counted non-daemon thread. |

---

## 9.2 Endpoint naming

`resolveQueueName(instanceId)` builds:

```
<queue or id>[.<group>][.<instanceId>]
```

| queue | group | appendInstanceIdToQueue | Result |
| :--- | :--- | :--- | :--- |
| `orders` | — | false | `orders` |
| `orders` | `workers` | false | `orders.workers` |
| `orders` | — | true | `orders.pod-a` |
| `orders` | `workers` | true | `orders.workers.pod-a` |
| *(unset)* | — | false | the container id |

`resolveTopics(instanceId)` returns the subscriptions unchanged unless `appendInstanceIdToTopics` is
set, in which case `orders/created` becomes `orders/created/pod-a`.

Whether the instance id is appended to the **queue** is the entire difference between fan-out and
competing consumers. Appending it to **topics** is a different thing: addressing one instance.

---

## 9.3 Startup

```
1.  Guards
      · concurrency <= max-transacted-sessions-per-connection    (transactional only)
      · EXECUTOR dispatch has a task executor
      · EXECUTOR dispatch is not combined with transactional=true
2.  DURABLE_QUEUE → provision the DMQ (if enabled), then provision the queue
    NON_DURABLE_QUEUE → session.createTemporaryQueue(name)
    DIRECT → no endpoint at all; subscriptions go on the session
3.  Create `concurrency` flows, stopped
4.  Add every topic subscription to the queue
5.  Start the flows
6.  Acquire the keep-alive reference
```

Steps 3→4→5 are ordered deliberately. A temporary queue does not exist on the broker until a flow
binds to it, so subscribing first fails with `503 Unknown Queue`; and creating flows *stopped* means
nothing can be delivered before the subscriptions are in place. If any step throws,
`releaseResources()` runs before the exception propagates, so a half-started container leaks nothing.

### Provisioning, and the two tolerated errors

`provision()` reports rather than hides the interesting cases:

| Subcode | Handling |
| :--- | :--- |
| `ENDPOINT_ALREADY_EXISTS` | debug log; expected on every restart |
| `ENDPOINT_PROPERTY_MISMATCH` | **warning** — the broker keeps the properties it already has |
| anything else | rethrown |

The mismatch warning matters because **the broker never reconfigures an existing endpoint**. Changing
`max-redelivery-count` or `quota-mb` in YAML has no effect on a queue that already exists; the queue
must be deleted, or changed through SEMP or the admin UI. Silently ignoring that makes a queue look
configured when it is not.

`addSubscription()` similarly tolerates `SUBSCRIPTION_ALREADY_PRESENT`. A durable queue keeps its
subscriptions between runs, so `400 Subscription Already Exists` is the normal steady state from the
second start onwards — not a failure.

---

## 9.4 Concurrency

`concurrency` is the number of **flows** bound to the endpoint. Each flow delivers independently, so
it is the container's parallelism.

What limits it:

| Constraint | Effect |
| :--- | :--- |
| `EXCLUSIVE` access type | One active consumer. Extra flows are standby at best, and a temporary endpoint rejects them outright. Warned at start. |
| `NON_DURABLE_QUEUE` | A temporary endpoint accepts exactly one flow **whatever access type was requested**. Clamped to 1, with a warning. |
| `transactional = true` | One transacted session per flow, and Solace caps them per connection. Asserted against `max-transacted-sessions-per-connection` at start. |

Parallel consumption therefore needs a **durable, non-exclusive** endpoint. That is exactly what
`POINT_TO_POINT` gives you.

Ordering: Solace preserves publish order per topic to a queue, but multiple flows consume
concurrently, so processing order is not guaranteed across flows. Use `concurrency: 1` with an
exclusive endpoint when strict ordering matters.

---

## 9.5 Dispatch modes

### `INLINE` (default)

The listener runs on the JCSMP delivery thread. Lowest latency, no hand-off, natural back-pressure —
that thread is not reading more from the socket while your listener runs. It is the only mode
compatible with transactions.

### `EXECUTOR`

Each flow gets a `FlowInvoker` — a `Runnable` submitted to `solaceListenerTaskExecutor`, with a
bounded `LinkedBlockingQueue` of `dispatch-queue-capacity` (default 256) between it and the delivery
thread.

```
JCSMP delivery thread ──put()──► [bounded queue] ──poll()──► solace-<id>-<n> worker
                        blocks when full
```

The bound is the point. `put` blocks, so a slow listener pushes back through the delivery thread onto
the broker's transport window. An unbounded queue would trade flow control for heap and fail later
and worse.

On stop the invoker drains what it has buffered, then after `shutdown-timeout` (default 10s) logs a
warning and interrupts the worker.

Use `EXECUTOR` when the listener blocks on something slow and you want the delivery thread free, or
when you need a specific thread pool (naming, MDC propagation, a bulkhead). Otherwise `INLINE`.

### Why `EXECUTOR` + `transactional` is rejected

A transacted session's `commit()` acknowledges **every message delivered on that session so far**,
not merely the one being handled. Buffering messages away from the delivery thread would let a commit
cover messages that have not been processed yet, and a rollback redeliver ones that have. The
container asserts this at startup with an explanatory `IllegalStateException` rather than allowing a
silent correctness hole.

---

## 9.6 Acknowledgement, settlement and errors

Solace calls deciding a message's fate **settling** it. A consumer has four answers, and choosing
between them is a judgement about the *failure*, not about the message:

| `SettlementOutcome` | Broker behaviour | Use when |
| :--- | :--- | :--- |
| `ACCEPTED` | Acknowledged, removed from the endpoint, never redelivered | The failure is recorded somewhere durable and a retry would not help |
| `FAILED` | Returned for redelivery, **incrementing the delivery count**; reaches the DMQ once `max-redelivery-count` is exhausted | A transient failure — a downstream timeout, a lock conflict — that a retry plausibly fixes |
| `REJECTED` | Straight to the dead message queue, **without** consuming redelivery attempts | The message will never succeed: it will not deserialise, it fails validation, it names something that does not exist |
| `NONE` | Nothing is sent; the message stays unacknowledged until the flow is rebound | Legacy behaviour. Prefer `FAILED`, which redelivers promptly and counts the attempt |

`FAILED` and `REJECTED` are the ones worth having. Without them a poison message can only be
discarded or retried five pointless times before the DMQ takes it; with them it is rejected on the
first attempt, and a genuinely transient failure is handed straight back instead of waiting for a
rebind.

### Configuring it

```yaml
solace:
  listener:
    error-outcome: FAILED          # ACCEPTED | FAILED | REJECTED | NONE
```

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers",
        topics = "orders/created", errorOutcome = "REJECTED")
public void onOrder(Order order) { … }
```

Precedence is the usual one: the annotation attribute beats `solace.listener.error-outcome`, which
beats the default.

### Negotiation at bind time

A flow may only send an outcome it **asked for when it bound**. The container handles this: it
requests `FAILED` and `REJECTED` whenever the resolved `error-outcome` is one of them, and does not
otherwise — no point asking the broker for capabilities you will never use.

Two cases need the switch set explicitly:

```yaml
solace:
  listener:
    negative-acknowledgement: true    # a custom error handler decides per message (see below)
    # negative-acknowledgement: false # broker or client too old to support settlement outcomes
```

If a settle call fails at runtime — almost always because the outcome was not negotiated — the
container logs an error naming this property and leaves the message for redelivery. It does not
rethrow: there is nothing useful to do with the exception on the JCSMP delivery thread, and the
broker will resolve the state anyway.

### The deprecated `ack-on-error`

`ack-on-error` still works and is still honoured, but only when `error-outcome` is unset:

| Old | New equivalent |
| :--- | :--- |
| `ack-on-error: true` (default) | `error-outcome: ACCEPTED` |
| `ack-on-error: false` | `error-outcome: NONE` |

Setting `error-outcome` takes precedence and is the preferred form. `ack-on-error: false` is worth
migrating to `FAILED` specifically: it redelivers promptly rather than waiting for a rebind, and it
counts the attempt, so `max-redelivery-count` actually bounds the retries.

### The invocation path

```java
try {
    messageListener.onMessage(message);
    message.ackMessage();                       // success
}
catch (Exception ex) {
    errorHandler.handleError(message, ex);      // report first, message still in hand
    settle(message, ex);                        // then decide its fate
}
```

The error handler runs **before** settlement, so it can record the failure, publish a copy elsewhere,
or increment a metric while the message is still available.

### Per-failure decisions

A single `error-outcome` is one answer for every failure. `SolaceListenerErrorHandler` has a second,
optional method for when the right answer depends on *why* it failed:

```java
factory.setErrorHandler(new SolaceListenerErrorHandler() {

    @Override
    public void handleError(BytesXMLMessage message, Exception exception) {
        deadLetters.record(message, exception);
    }

    @Override
    public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
        if (exception instanceof SolaceMessagingException) {
            return SettlementOutcome.REJECTED;   // will never deserialise
        }
        if (exception instanceof TimeoutException) {
            return SettlementOutcome.FAILED;     // transient
        }
        return null;                             // anything else: the container decides
    }
});
```

`resolveOutcome` has a `default` returning `null`, so an error handler written as a lambda keeps
working unchanged. Because the container cannot know in advance what a handler will decide, **set
`negative-acknowledgement: true`** when using it — otherwise the flow will not have negotiated the
outcomes the handler wants to send.

An exception thrown by `resolveOutcome` is logged at warning level and the configured outcome is used.

### Transactional flows

Settlement does not apply. The listener runs inside a `TransactionTemplate`; a thrown exception rolls
the transaction back, which un-acknowledges the message and discards anything the listener published.
The broker redelivers, subject to `max-redelivery-count`. `error-outcome`, `ack-on-error` and
`resolveOutcome` are all ignored, and the flow does not negotiate settlement outcomes. The error
handler is still called, after the rollback, for logging.

### Custom error handling

```java
@Bean
DefaultSolaceListenerContainerFactory solaceListenerContainerFactory(
        SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
        SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds,
        SolaceProperties properties, @Qualifier("solaceTemplate") SolaceTemplate<Object> template,
        SolaceTransactionManager transactionManager) {

    DefaultSolaceListenerContainerFactory factory = new DefaultSolaceListenerContainerFactory(
            sessionFactory, converter, headerMapper, instanceIds, properties.getListener());
    factory.setReplyTemplate(template);
    factory.setTransactionManager(transactionManager);
    factory.setErrorHandler((message, ex) -> {
        deadLetterService.record(message.getMessageId(), ex);
        meterRegistry.counter("solace.listener.errors").increment();
    });
    return factory;
}
```

Declaring a bean of this name replaces the auto-configured factory for every listener; declaring an
extra one under a different name and pointing individual listeners at it with
`containerFactory = "…"` is usually better.

---

## 9.7 Delivery count

`SolaceRecord.getDeliveryCount()` is how many times the broker has delivered this message: `1` on the
first attempt, so anything above 1 is a retry.

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers",
        topics = "orders/created")
public void onOrder(SolaceRecord<Order> record) {
    if (record.getDeliveryCount() > 3) {
        log.warn("Order {} has failed {} times", record.getPayload().id(), record.getDeliveryCount());
    }
    process(record.getPayload());
}
```

It is also on the header map as `SolaceHeaders.DELIVERY_COUNT` (`solace_deliveryCount`), so
`@Header(SolaceHeaders.DELIVERY_COUNT) int attempt` works without taking a whole record.

**Check `isDeliveryCountSupported()` before branching on the number.** Delivery counts are a broker
feature negotiated per message; where they are unavailable the count is `-1`, and every
`>= n` comparison silently reads that as a first delivery:

```java
if (record.isDeliveryCountSupported() && record.getDeliveryCount() > 3) { … }
```

The library never lets this throw: JCSMP raises `UnsupportedOperationException` from
`getDeliveryCount()` when the feature is absent, and `DefaultSolaceHeaderMapper.deliveryCountOf`
guards both that and the capability check, degrading to `-1`.

`isRedelivered()` remains the always-available boolean. Use it when all you need is "have I seen this
before"; use the count when the policy depends on *how many times*.

### The two together

Delivery count and settlement are complementary, and the pair is what makes a give-up policy
expressible:

```java
public void onOrder(SolaceRecord<Order> record) {
    process(record.getPayload());          // throws on failure
}
```
```java
@Override
public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
    int attempt = DefaultSolaceHeaderMapper.deliveryCountOf(message);
    if (attempt >= 3) {
        return SettlementOutcome.REJECTED;   // three strikes: stop retrying, send it to the DMQ
    }
    return SettlementOutcome.FAILED;         // hand it back
}
```

That was not expressible before either feature existed: `isRedelivered()` could not count, and there
was no way to reject a message early.

---

## 9.8 Flow events

A flow is a consumer's binding to an endpoint, and its lifecycle is otherwise invisible: a flow can
go down and come back without a single message being lost or a single log line appearing. Flow events
are where a reconnect, a lost bind, or a change of active consumer becomes observable.

| Event | Meaning |
| :--- | :--- |
| `UP` | The flow bound and is consuming |
| `DOWN` | Lost and **will not recover on its own** — endpoint deleted, bind rejected, unrecoverable error. The container must be restarted |
| `RECONNECTING` | Lost, JCSMP is retrying. **Consumption has stopped for now** |
| `RECONNECTED` | The retry succeeded, consumption resumed |
| `ACTIVE` | This flow is *the* consumer on an exclusive endpoint |
| `INACTIVE` | This flow is standing by; another instance holds the endpoint |
| `UNKNOWN` | A JCSMP event this library does not model — reported rather than swallowed |

### Logging comes free

Every container passes a `FlowEventHandler` and logs each event at the level an operator needs:
`DOWN` as **error** (it will not fix itself), `RECONNECTING` as **warning** (delivery has stopped),
the rest at info.

```
WARN  Flow 0 of container 'orders' on endpoint 'orders.workers' is reconnecting; consumption has stopped.
INFO  Flow 0 of container 'orders' reconnected to endpoint 'orders.workers'
```

### Acting on them

```java
@Bean
SolaceFlowListener solaceFlowListener(AlertService alerts) {
    return args -> {
        if (args.getEvent() == SolaceFlowEvent.DOWN) {
            alerts.page("Solace flow down on " + args.getEndpoint(), args.getException());
        }
    };
}
```

A single bean of this type is picked up by auto-configuration and given to every container.
`SolaceFlowEventArgs` carries the container id, flow index, endpoint, event, JCSMP's description, the
exception and the broker response code.

The callback runs on a JCSMP notification thread, so it must be quick and must not block. Exceptions
are logged and swallowed rather than propagating into JCSMP, where they would be dropped anyway.

### Leader election, almost for free

On an **exclusive** endpoint the broker tells exactly one flow it is the active consumer. That is a
leader election with no extra coordination:

```java
@SolaceListener(id = "scheduler", pattern = "POINT_TO_POINT", queue = "scheduler",
        topics = "scheduler/tick", concurrency = "1")
public void onTick(Tick tick) { … }
```
```yaml
solace:
  listener:
    endpoint:
      access-type: EXCLUSIVE
```
```java
@Bean
SolaceFlowListener leadershipListener(Scheduler scheduler) {
    return args -> {
        switch (args.getEvent()) {
            case ACTIVE   -> scheduler.becomeLeader();
            case INACTIVE -> scheduler.standDown();
            default       -> { }
        }
    };
}
```

Active flow indication is requested automatically when the access type is `EXCLUSIVE`, and not
otherwise — see [9.9](#99-flow-tuning). Without it the broker never sends these events and a standby
instance has no way to learn it has taken over.

### Container state

Two accessors on `DefaultSolaceMessageListenerContainer` derive from flow events:

| | |
| :--- | :--- |
| `isDegraded()` | Any flow down or reconnecting. **This is the difference between "running" and "actually consuming"** — a container stays running throughout a reconnect |
| `isActive()` | This container is the active consumer. On a non-exclusive endpoint, or with indication off, it simply mirrors "running and not degraded" |
| `getLastFlowEvent()` | The most recent event on any flow, or `null` before the first |

`INACTIVE` is deliberately **not** degraded: a standby flow on an exclusive endpoint is healthy and
working as designed, and treating it as degraded would fail the health check of every instance that
is not the leader.

The Actuator health indicator uses `isDegraded()`, which is what lets it distinguish a container that
is running from one that is running but not consuming — see
[16.3](16-operations.md#163-actuator-health).

**Flow events are not the whole story.** A flow rides on a session, and JCSMP reconnects a *session*
transparently: a network blip can stop all traffic for seconds while every flow survives and raises
nothing. Session events cover that layer — see [13.7](13-multi-instance.md#137-session-events).

---

## 9.9 Flow tuning

`solace.listener.flow.*` applies to every flow the container binds. **Every value is unset by
default**, and a property reaches `ConsumerFlowProperties` only once given a value — so an empty block
means JCSMP's own defaults, exactly as before this block existed.

```yaml
solace:
  listener:
    flow:
      transport-window-size: 255
      ack-threshold: 60
      ack-timer: 1s
      windowed-ack-max-size:
      reconnect-tries: -1
      reconnect-retry-interval: 3s
      active-flow-indication:      # unset derives it from the access type
```

| Property | JCSMP default | Effect |
| :--- | :--- | :--- |
| `transport-window-size` | 255 | Messages the broker may have in flight to this flow before waiting for an acknowledgement. **The primary throughput knob for guaranteed messaging.** Raising it helps a fast consumer on a high-latency link and costs broker memory per flow; lowering it bounds how many messages a failure can put back for redelivery |
| `ack-threshold` | 60 | Percentage of the window at which the flow acknowledges. Higher means fewer round-trips and more redelivery if the flow drops |
| `ack-timer` | 1s | How long the flow waits before acknowledging when the threshold has not been reached — the floor on ack latency for a slow trickle |
| `windowed-ack-max-size` | JCSMP's | Maximum messages acknowledged in one transmission |
| `reconnect-tries` | JCSMP's | How many times a lost **flow** is retried before `FLOW_DOWN`. `-1` retries forever. Separate from session reconnection under `solace.java.*` |
| `reconnect-retry-interval` | JCSMP's | Wait between flow reconnection attempts |
| `active-flow-indication` | derived | Ask the broker to say when this flow becomes the active consumer. Unset enables it for `EXCLUSIVE` endpoints and not otherwise |
| `no-local` | `false` | Suppress delivery of messages published on the **same connection** — see below |

Note the distinction from `solace.listener.endpoint.*`: those are **endpoint** properties, applied
only when a queue is first provisioned and thereafter ignored by the broker. These are **flow**
properties, applied on every bind — so changing them takes effect on the next restart, with no need
to touch the queue.

### Per-listener tuning

There is no `@SolaceListener` attribute for these — there are too many, and they are rarely
per-listener. Declare a second container factory instead:

```java
@Bean
DefaultSolaceListenerContainerFactory bulkListenerContainerFactory(
        SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
        SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds) {

    ContainerProperties properties = new ContainerProperties();
    properties.getFlow().setTransportWindowSize(1024);
    properties.getFlow().setAckThreshold(80);

    return new DefaultSolaceListenerContainerFactory(sessionFactory, converter, headerMapper,
            instanceIds, properties);
}
```
```java
@SolaceListener(queue = "bulk", topics = "bulk/>", containerFactory = "bulkListenerContainerFactory")
public void onBulk(BulkEvent event) { … }
```

### `no-local`: don't hear your own echo

A service that both publishes to a topic and subscribes to it receives its own messages, because the
subscription does not care who published. `no-local` suppresses that:

```yaml
solace:
  listener:
    flow:
      no-local: true
```

Two things to know, both of which surprise people:

- **Solace matches on the client connection, not on the application.** The library shares one session
  for publishing and non-transactional consuming, so this works — but a **transactional** container
  opens its own connection, so its publishes are already on a different connection and `no-local` has
  no effect there.
- **It is a per-flow filter, not a discard.** On a shared durable queue the message is not delivered
  to *this* instance, but it is still delivered to another one. It suppresses local delivery; it does
  not remove the message.

### Where to start

Leave all of it alone until you have a measured problem. Then:

- **Consumer is idle waiting for the broker** — raise `transport-window-size`.
- **Acknowledgement round-trips dominate** — raise `ack-threshold`, accepting more redelivery on a
  drop.
- **Low-rate flow with high ack latency** — lower `ack-timer`.
- **Flows going `DOWN` during brief broker blips** — raise `reconnect-tries`.

---

## 9.10 Topic dispatch — several methods, one endpoint

Every `@SolaceListener` normally gets its own endpoint and its own flows. A service subscribing to
twenty related topics therefore pays for twenty queues, twenty provisioning rounds and twenty binds.
Topic dispatch lets those methods share one endpoint:

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "notifications", group = "v1",
        topics = "routed/deployment/>", topicDispatch = "true")
public void onDeployment(Notification notification) { … }

@SolaceListener(pattern = "POINT_TO_POINT", queue = "notifications", group = "v1",
        topics = "routed/incident/*", topicDispatch = "true")
public void onIncident(Notification notification) { … }

// declared LAST: the first match wins
@SolaceListener(pattern = "POINT_TO_POINT", queue = "notifications", group = "v1",
        topics = "routed/>", topicDispatch = "true")
public void onAnythingElse(SolaceRecord<Notification> record) { … }
```

One durable queue `notifications.v1`, carrying the union of the three subscriptions. Each delivered
message is routed to the method whose subscription matched — and **each method keeps its own payload
type**, which is the point.

### It is opt-in

`topicDispatch = "true"` is required on every member. Merging listeners that merely happen to share a
queue name would change what an existing application does, silently.

Members are grouped by resolved **queue + group + containerFactory** — the things that decide which
physical endpoint each would have bound on its own.

### First match wins, in declaration order

Matching is by declaration order, **not by specificity**. A catch-all declared before the specific
subscriptions swallows everything:

```java
// WRONG — onAnythingElse takes every message
@SolaceListener(topics = "routed/>",           topicDispatch = "true") void onAnythingElse(…)
@SolaceListener(topics = "routed/incident/*",  topicDispatch = "true") void onIncident(…)
```

Declare the specific ones first, or do not overlap.

### Members must agree on the endpoint

They share one flow set, so a setting on a later member could only be honoured by ignoring the first.
A disagreement on `pattern`, `endpointMode`, `concurrency`, `transactional`, `selector` or
`accessType` **fails at startup**, naming both methods, rather than being silently dropped.

The container id, subscriptions aside, comes from the first member — give that one an explicit `id`
if you want to find the container in the registry or in a log line.

### Unmatched messages are acknowledged

A message matching no target is logged at warning and acknowledged. Failing it would redeliver
forever: nothing about a retry makes a subscription match that did not match the first time, and the
endpoint would fill with messages no method wants.

The warning is the useful signal — it usually means the durable endpoint carries a subscription no
target claims any more, left behind by an earlier version of the code. **A durable endpoint keeps its
subscriptions across restarts**, so removing a `@SolaceListener` does not remove its subscription from
the queue.

### Wildcard rules

Client-side matching follows the broker's rules exactly (`SolaceTopicMatcher`):

| Pattern | Matches | Does not match |
| :--- | :--- | :--- |
| `orders/*` | `orders/created` | `orders`, `orders/eu/created` |
| `orders/>` | `orders/created`, `orders/eu/created` | `orders` |
| `orders/cr*` | `orders/created`, `orders/cr` | `orders/cancelled` |
| `>` | everything | — |

`*` is exactly one level; `>` is one or more trailing levels and only means anything as the final
character.

### When not to use it

Sharing an endpoint means sharing its **concurrency, its transaction setting and its backlog**. A slow
handler on one topic delays every other topic on that queue. Give a topic its own endpoint when it
needs its own throughput or its own failure isolation — the same trade as
[splitting a reply destination](10-request-reply.md#106-when-to-split-a-reply-destination).

---

## 9.11 Message replay

Replay asks the broker to re-deliver messages it has already spooled. It turns the broker into a
short-term event store: rebuild a projection after a bug, or bring a new service online with history
rather than only new events.

### As an operation (preferred)

```java
DefaultSolaceMessageListenerContainer container =
        (DefaultSolaceMessageListenerContainer) registry.getListenerContainer("orders");

container.replay(ReplayStartPoint.beginning());
container.replay(ReplayStartPoint.from(Instant.now().minus(Duration.ofHours(6))));
container.replay(null);                       // back to live delivery on the next bind
```

The container stops and restarts, because a replay start point can only be given to a flow as it
binds. Everything spooled from that point is delivered again, then live delivery resumes.

Replay is an **operational act**, not a deployment-time setting, which is why this is the preferred
form.

### As configuration

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "rebuild",
        topics = "orders/>", replayFrom = "BEGINNING")
public void rebuild(Order order) { … }
```

`BEGINNING`, or an ISO-8601 instant such as `2026-08-23T10:15:30Z`. Placeholders work
(`replayFrom = "${app.replay-from:}"`), and an unparseable value fails at startup naming what was
expected.

**Leaving a start point in an annotation replays on every restart.** That is occasionally what you
want — a rebuild-on-boot projection — and usually not.

### Three things to be deliberate about

1. **It affects the endpoint, not this instance.** On a shared queue every consumer of that endpoint
   receives the replayed messages. It is not a private read.
2. **Handlers see the messages again.** Anything with side effects must be idempotent — and
   `isRedelivered()` does **not** distinguish a replayed message from a first delivery.
3. **A replay the broker cannot satisfy fails the flow.** Replay must be enabled for the Message VPN
   and the replay log must still cover the period asked for. The failure arrives as a `DOWN`
   [flow event](#98-flow-events) — an error in the log and a DOWN health status — not as an exception
   from the call that requested it.

That third point is why flow events were the prerequisite for this feature: without them a failed
replay would leave a silently non-consuming container.

### Replaying onto a private endpoint

The usual way to avoid disturbing live consumers is to replay onto an endpoint nobody else uses —
a separate group, so the replayed stream lands on its own queue:

```java
@SolaceListener(id = "projection-rebuild", pattern = "POINT_TO_POINT",
        queue = "orders", group = "rebuild-${HOSTNAME:local}",
        topics = "orders/>", autoStartup = "false")
public void rebuild(Order order) { … }
```

Registered but idle; start it with a replay when the rebuild is wanted.

---

## 9.12 Redelivery and the dead message queue

```yaml
solace:
  listener:
    ack-on-error: false
    endpoint:
      max-redelivery-count: 5
      dead-message-queue:
        provision: true
```

With this, a failing message is redelivered five times and then moved to `#DEAD_MSG_QUEUE`.

Four things to know:

1. `max-redelivery-count: 0` means **redeliver forever**, not "never redeliver". `error-outcome:
   REJECTED` is the escape hatch that does not depend on it — it sends the message to the DMQ
   immediately.
2. The message must be **DMQ-eligible** (`solace.template.dmq-eligible`, default `true`) or it is
   discarded instead of moved.
3. `#DEAD_MSG_QUEUE` is the **fixed** name the broker routes to. A message VPN has exactly one.
   Renaming it means the broker will not use it.
4. The DMQ is provisioned with `respects-ttl = false` on purpose: a message that arrived because it
   expired must not immediately expire again in the queue meant to preserve it.

`max-redelivery-count` is an endpoint property, so it only applies at first provision. On an existing
queue you will see the property-mismatch warning and must change it on the broker.

---

## 9.13 Lifecycle and manual control

Containers are lifecycled as a group by `SolaceListenerEndpointRegistry`, itself a `SmartLifecycle`
bean. To control one by hand:

```java
@Autowired SolaceListenerEndpointRegistry registry;

registry.getListenerContainer("orders").stop();
registry.getListenerContainer("orders").start();

registry.getListenerContainerIds();      // every registered id
registry.getListenerContainers();        // every container
```

Declare the listener with `autoStartup = "false"` to register it without starting it — useful for a
backfill job, or a consumer that must not run until a warm-up completes.

`DefaultSolaceMessageListenerContainer.getResolvedQueueName()` returns the physical endpoint name
once started, which is the value worth logging or exposing: it is the one that includes the group and
instance id.

---

## 9.14 Programmatic registration

`@SolaceListener` is a convenience over an API you can use directly — useful when endpoints are
discovered at runtime:

```java
SolaceListenerEndpoint endpoint = new SolaceListenerEndpoint();
endpoint.setId("orders-" + tenant);
endpoint.setPattern(ExchangePattern.POINT_TO_POINT);
endpoint.setQueue("orders-" + tenant);
endpoint.setTopics(List.of("orders/" + tenant + "/>"));
endpoint.setConcurrency(4);
endpoint.applyPatternDefaults();

SolaceMessageListenerContainer container =
        registry.registerListenerContainer(endpoint, containerFactory);

container.setupMessageListener(new RecordSolaceListenerAdapter<>(
        (SolaceRecord<Order> record) -> handle(record.getPayload()),
        messageConverter, headerMapper, Order.class, null, replyTemplate));

container.start();
```

Remember to call `applyPatternDefaults()` yourself — the post-processor does it for annotated
listeners, nothing does it here.

---

## 9.15 The keep-alive thread

Every JCSMP thread is a daemon thread. A listener-only application with no web server would start,
register everything, and exit immediately — the JVM has no non-daemon thread to keep it alive.

`ContainerKeepAlive` is a reference-counted, package-private, non-daemon thread that parks (it joins
itself, which blocks until interrupted). The first container that starts with `keep-alive: true`
starts it; the last to stop interrupts it. The JVM therefore lives exactly as long as something is
consuming.

Set `solace.listener.keep-alive: false` when the application already has a non-daemon thread — a
WebFlux or MVC service does — so shutdown is governed by the web server alone.

---

**Next:** [10. Request-reply](10-request-reply.md)
