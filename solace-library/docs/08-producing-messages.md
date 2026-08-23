# 8. Producing messages

`SolaceTemplate<T>` is the send side, and `SolaceOperations<T>` is the interface it implements —
the counterpart of `KafkaTemplate` / `KafkaOperations`.

---

## 8.1 `SolaceOperations<T>`

| Method | Use it when |
| :--- | :--- |
| `void send(T payload)` | The template has a `defaultDestination`. Throws if it does not. |
| `void send(String destination, T payload)` | The common case. |
| `void send(String destination, String correlationId, T payload)` | You are correlating by hand — a manual reply, or a saga step. |
| `void send(String destination, T payload, Map<String,Object> headers)` | You need user properties or Solace header fields. |
| `void send(Message<?> message)` | You already have a Spring `Message`. The destination comes from its `solace_targetDestination` header, else the template default. |
| `void send(Destination destination, XMLMessage message)` | Full control: you built the JCSMP message yourself. No conversion, no header mapping. |
| `<R> R executeInTransaction(TransactionCallback<T,R> callback)` | A programmatic local transaction without a `TransactionTemplate`. |

Destination strings are **topics** unless prefixed `queue:`:

```java
solace.send("orders/created", order);        // topic
solace.send("queue:orders.dlq", order);      // queue, directly
```

The prefix is `DefaultSolaceHeaderMapper.QUEUE_PREFIX` and applies to `solace_replyTo` too.

---

## 8.2 Template defaults

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

## 8.3 What `send` actually does

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
transaction with no API difference. See [11. Transactions](11-transactions.md).

**Publish acknowledgement.** With `PERSISTENT` delivery the broker acknowledges asynchronously.
`DefaultSolaceSessionFactory.LoggingPublishEventHandler` logs the outcome; a `send` that returns has
handed the message to JCSMP, not necessarily had it accepted by the broker. Wrap in a transaction
when you need the commit to mean "the broker has it".

---

## 8.4 Headers

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
[12. Conversion and headers](12-conversion-and-headers.md).

---

## 8.5 Sending a Spring `Message`

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

## 8.6 Programmatic transactions

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
standard Spring semantics; see [11. Transactions](11-transactions.md).

---

## 8.7 Full control

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

## 8.8 Single, multiple, batch

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

**Next:** [9. Consuming messages](09-consuming-messages.md)
