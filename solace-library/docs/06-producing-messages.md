# 6. Producing messages

`SolaceTemplate<T>` is the send side, and `SolaceOperations<T>` is the interface it implements —
the counterpart of `KafkaTemplate` / `KafkaOperations`.

---

## 6.1 `SolaceOperations<T>`

| Method | Use it when |
| :--- | :--- |
| `void send(T payload)` | The template has a `defaultDestination`. Throws if it does not. |
| `void send(String destination, T payload)` | The common case. |
| `void send(String destination, String correlationId, T payload)` | You are correlating by hand — a manual reply, or a saga step. |
| `void send(String destination, T payload, Map<String,Object> headers)` | You need user properties or Solace header fields. |
| `void send(Message<?> message)` | You already have a Spring `Message`. The destination comes from its `solace_targetDestination` header, else the template default. |
| `void send(Destination destination, XMLMessage message)` | Full control: you built the JCSMP message yourself. No conversion, no header mapping. |
| `<R> R executeInTransaction(TransactionCallback<T,R> callback)` | A programmatic local transaction without a `TransactionTemplate`. |
| `<B> SolaceBrowser<B> browse(String queue, Class<B> type)` | Read a queue **without consuming it** — see [6.9](#69-browsing-a-queue). |
| `<B> SolaceBrowser<B> browse(BrowseSpec spec, Class<B> type)` | The same, with a selector or a wait timeout. |

Destination strings are **topics** unless prefixed `queue:`:

```java
solace.send("orders/created", order);        // topic
solace.send("queue:orders.dlq", order);      // queue, directly
```

The prefix is `DefaultSolaceHeaderMapper.QUEUE_PREFIX` and applies to `solace_replyTo` too.

---

## 6.2 Template defaults

Set from `solace.template.*` on the auto-configured bean, and settable on any template you build.
They apply to every message the template sends.

| Property | Default | Effect |
| :--- | :--- | :--- |
| `defaultDestination` | — | Used by `send(payload)` and by `send(Message)` with no target header |
| `deliveryMode` | `PERSISTENT` | `PERSISTENT` spools and is acknowledged; `DIRECT` is at-most-once and cannot be consumed from a queue |
| `timeToLive` | `0` | Milliseconds; `0` never expires. Only honoured on an endpoint with `respects-ttl` |
| `priority` | unset | 0–255 |
| `dmqEligible` | `true` | Whether an expired or max-redelivered message moves to the DMQ rather than being discarded |
| `messageConverter` | Jackson | Body conversion |
| `headerMapper` | `DefaultSolaceHeaderMapper` | Header conversion |

A second template with different defaults is an ordinary bean:

```java
@Bean
SolaceTemplate<Object> telemetryTemplate(SolaceSessionFactory sessionFactory,
                                         SolaceMessageConverter converter) {
    SolaceTemplate<Object> template = new SolaceTemplate<>(sessionFactory, converter);
    template.setDeliveryMode(DeliveryMode.DIRECT);   // lossy is fine, latency is not
    template.setTimeToLive(5_000);
    return template;
}
```

Remember `solaceTemplate` is `@Primary`, so injections without a qualifier still get the default one.

---

## 6.3 What `send` actually does

```
send(destination, payload, headers)
  │
  ├─ createMessage(payload, headers)
  │     ├─ messageConverter.toMessage(payload)      → BytesMessage, body in the ATTACHMENT
  │     ├─ setDeliveryMode / setTimeToLive / setPriority / setDMQEligible
  │     └─ headerMapper.fromHeaders(headers, message)
  │
  ├─ producer()
  │     ├─ transaction active?  → the bound SolaceResourceHolder's producer
  │     └─ otherwise            → sessionFactory.getSharedProducer()
  │
  └─ producer.send(message, destination)
```

**The body goes in the binary attachment.** `BytesMessage.setData()` writes the attachment;
`BytesXMLMessage.getBytes()` reads the *XML content part*, which is a different section and comes
back empty. The converter reads the attachment first and falls back to the XML content part for
interoperability with producers that use it. This asymmetry is the single most common cause of an
empty-payload bug when integrating with a non-JCSMP publisher.

**Transaction awareness is automatic.** `isTransactionActive()` consults
`TransactionSynchronizationManager`, so the same call publishes immediately or enlists in an ambient
transaction with no API difference. See [9. Transactions](09-transactions.md).

**Publish acknowledgement.** With `PERSISTENT` delivery the broker acknowledges asynchronously.
`DefaultSolaceSessionFactory.LoggingPublishEventHandler` logs the outcome; a `send` that returns has
handed the message to JCSMP, not necessarily had it accepted by the broker. Wrap in a transaction
when you need the commit to mean "the broker has it".

---

## 6.4 Headers

Set Solace message fields through the constants in `SolaceHeaders`; anything else becomes an SDT
user property.

```java
solace.send("orders/created", order, Map.of(
        SolaceHeaders.CORRELATION_ID, orderId,
        SolaceHeaders.REPLY_TO,       "queue:order-acks",
        "tenant",                     "acme",            // user property
        "schemaVersion",              2));               // user property
```

| Constant | Maps to |
| :--- | :--- |
| `SolaceHeaders.CORRELATION_ID` | `setCorrelationId` |
| `SolaceHeaders.REPLY_TO` | `setReplyTo` (`queue:` prefix ⇒ queue, else topic) |
| `SolaceHeaders.APPLICATION_MESSAGE_ID` | `setApplicationMessageId` |
| `SolaceHeaders.TIME_TO_LIVE` | `setTimeToLive` — per message, overriding the template |
| `SolaceHeaders.PRIORITY` | `setPriority` |
| `SolaceHeaders.TARGET_DESTINATION` | Not written to the message; used to route `send(Message)` and a listener's reply |
| anything else | An SDT user property, readable with `@Header("name")` |

Never written outbound: `solace_rawMessage`, `solace_destination`, `solace_redelivered`,
`solace_targetDestination`, and Spring's own `id` and `timestamp`. Full details in
[10. Conversion and headers](10-conversion-and-headers.md).

---

## 6.5 Sending a Spring `Message`

```java
Message<Order> message = MessageBuilder.withPayload(order)
        .setHeader(SolaceHeaders.TARGET_DESTINATION, "orders/created")
        .setHeader(SolaceHeaders.CORRELATION_ID, orderId)
        .setHeader("tenant", "acme")
        .build();

solace.send(message);
```

Useful when the destination travels with the message — a router, a saga step, or output from a
Spring Integration flow.

---

## 6.6 Programmatic transactions

`executeInTransaction` is the lightweight option when you do not want a `TransactionTemplate`:

```java
solace.executeInTransaction(operations -> {
    operations.send("orders/created", order);
    operations.send("audit/order", audit(order));
    return null;                    // both, or neither
});
```

If a transaction is **already** bound to the thread, the callback simply runs inside it — it does not
open a nested one. Otherwise a `TransactedSession` is created, bound, committed on normal return,
rolled back on a `RuntimeException`, and closed either way.

`TransactionTemplate` and `@Transactional("solaceTransactionManager")` do the same thing with the
standard Spring semantics; see [9. Transactions](09-transactions.md).

---

## 6.7 Full control

When you need a JCSMP feature the template does not surface, build the message yourself:

```java
BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
message.setData(bytes);
message.setDeliveryMode(DeliveryMode.PERSISTENT);
message.setUserData(userData);                       // not surfaced by the template
solace.send(JCSMPFactory.onlyInstance().createTopic("orders/created"), message);
```

That overload does no conversion and no header mapping — you own the message entirely. It still uses
the transaction-aware producer, so it participates in an ambient transaction.

---

## 6.8 Single, multiple, batch

Three send shapes worth naming explicitly, because they behave differently on failure:

| | What it means | Failure behaviour |
| :--- | :--- | :--- |
| **single** | one message | it published, or it did not |
| **multiple** | N independent `send` calls | each is on the wire as it is sent; a failure part way through leaves the earlier ones delivered |
| **batch** | N sends inside one local transaction | nothing reaches the broker until commit; consumers see all of them or none |

```java
// multiple
orders.forEach(order -> solace.send("orders/created", order));

// batch
transactionTemplate.executeWithoutResult(status ->
        orders.forEach(order -> solace.send("orders/created", order)));
```

Batching is not a throughput optimisation here — it is an atomicity guarantee. A transacted session
adds a round trip at commit.

---

## 6.9 Browsing a queue

Browsing reads what is spooled on an endpoint **without acknowledging it**. The messages stay on the
queue and are still delivered to whatever consumer is bound. It is the operator's view: what is on the
dead message queue, why a backlog is not draining, what a poison message actually contains.

```java
try (SolaceBrowser<Order> browser = solace.browse("#DEAD_MSG_QUEUE", Order.class)) {
    browser.stream(100).forEach(record ->
            log.info("dead: {} after {} deliveries",
                    record.getPayload(), record.getDeliveryCount()));
}
```

`SolaceBrowser` is `AutoCloseable` and **must be closed** — it holds a bind on the endpoint, which
counts against that endpoint's bind limit, so an unclosed browser can keep a real consumer from
binding. `close()` throws nothing, so try-with-resources needs no catch.

| Method | |
| :--- | :--- |
| `Optional<SolaceRecord<T>> next()` | The next message, or empty at the end of the queue |
| `List<SolaceRecord<T>> take(int max)` | Up to `max`; a shorter list means the queue ran out |
| `Stream<SolaceRecord<T>> stream()` | Lazy, unbounded — ends when the queue does |
| `Stream<SolaceRecord<T>> stream(int limit)` | Lazy, bounded. Prefer this |
| `void remove(SolaceRecord<T> record)` | **Destructive** — deletes the message from the queue |
| `void close()` | Release the bind. Idempotent |

The stream is genuinely lazy, so `findFirst()` or `.limit(n)` stops the browse rather than draining
the queue first.

### `BrowseSpec`

| Property | Default | |
| :--- | :--- | :--- |
| `queue` | — | The queue name. A **queue**, not a topic: browsing reads a spooled endpoint |
| `selector` | — | Broker-side filter, so unmatched messages are never transferred |
| `waitTimeout` | `0` | How long `next()` waits. Zero does not block, which is what a drain loop wants |
| `transportWindowSize` | JCSMP's | Messages in flight; raising it speeds up a long browse |

```java
BrowseSpec spec = BrowseSpec.of("orders.workers", "priority > 5");
spec.setWaitTimeout(Duration.ofSeconds(1));
try (SolaceBrowser<Order> browser = solace.browse(spec, Order.class)) { … }
```

### What browsing is not

- **Not a consumer.** No acknowledgement, no redelivery, no transaction, no notification of new
  arrivals. A browser sees the queue as it walks it.
- **Not a depth call.** The client API has no "how many messages" question — that is SEMP. Counting
  means walking, which is why bounding the walk matters on a large backlog.
- **Not available on every endpoint.** A browser binds like a consumer, so an **exclusive** endpoint
  that already has its one consumer will reject it, and a **non-durable** queue belongs to the client
  that created it. In practice browsing is for durable queues, and the DMQ above all.

### `remove` is the one destructive operation

```java
try (SolaceBrowser<Object> browser = solace.browse("#DEAD_MSG_QUEUE", Object.class)) {
    browser.stream(50)
            .filter(record -> isUnrecoverable(record.getPayload()))
            .forEach(browser::remove);       // gone; no consumer will ever see these
}
```

That is what makes browsing useful operationally — inspect a poison message, then drop it — and the
reason not to point a browse at a live work queue by accident.

---

**Previous:** [5. Exchange patterns](05-exchange-patterns.md)  ·  [Index](00-index.md)  ·  **Next:** [7. Consuming messages](07-consuming-messages.md)
