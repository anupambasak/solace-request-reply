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
| `SolaceListenerErrorHandler` | `void handleError(BytesXMLMessage, Exception)`. Defaults to logging. |
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

## 9.6 Acknowledgement and errors

### Non-transactional flows

Flows use `SUPPORTED_MESSAGE_ACK_CLIENT`, and the container acknowledges explicitly:

```java
try {
    messageListener.onMessage(message);
    message.ackMessage();                       // success
}
catch (Exception ex) {
    errorHandler.handleError(message, ex);
    if (containerProperties.isAckOnError()) {
        message.ackMessage();                   // give up on this message
    }
}
```

- `ack-on-error: true` (default) — the message is acknowledged even after a failure. It will not be
  redelivered. Correct when the error handler has recorded the failure somewhere durable.
- `ack-on-error: false` — the message is left unacknowledged and the broker redelivers.
  **Combine this with `max-redelivery-count` and a DMQ**, or a poison message loops forever.

### Transactional flows

`ack-on-error` does not apply. The listener runs inside a `TransactionTemplate`; a thrown exception
rolls the transaction back, which un-acknowledges the message and discards anything the listener
published. The broker redelivers, subject to `max-redelivery-count`. The error handler is still
called, after the rollback, for logging.

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

## 9.7 Redelivery and the dead message queue

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

1. `max-redelivery-count: 0` means **redeliver forever**, not "never redeliver".
2. The message must be **DMQ-eligible** (`solace.template.dmq-eligible`, default `true`) or it is
   discarded instead of moved.
3. `#DEAD_MSG_QUEUE` is the **fixed** name the broker routes to. A message VPN has exactly one.
   Renaming it means the broker will not use it.
4. The DMQ is provisioned with `respects-ttl = false` on purpose: a message that arrived because it
   expired must not immediately expire again in the queue meant to preserve it.

`max-redelivery-count` is an endpoint property, so it only applies at first provision. On an existing
queue you will see the property-mismatch warning and must change it on the broker.

---

## 9.8 Lifecycle and manual control

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

## 9.9 Programmatic registration

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

## 9.10 The keep-alive thread

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
