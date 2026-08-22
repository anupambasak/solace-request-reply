# solace-library

Spring messaging for **Solace PubSub+**, modelled on
[Spring for Apache Kafka](https://spring.io/projects/spring-kafka) and built directly on the Solace
[JCSMP](https://docs.solace.com/API/API-Developer-Guide-Java/Java-API-Overview.htm) API.

```java
@SolaceListener(pattern = "POINT_TO_POINT", topics = "work/submit",
        queue = "work", group = "workers", transactional = "true")
public void onWork(WorkItem item) {
    process(item);
}
```

```java
@Autowired SolaceTemplate<Object> solace;
solace.send("events/created", event);

@Autowired ReplyingSolaceTemplate replying;
Ack ack = replying.sendAndReceive("orders/place", order, Ack.class).get();
```

Put the module on the classpath, set `solace.java.host`, and the auto-configuration does the rest —
no `@EnableSolace` required.

---

## What it gives you

| Spring for Apache Kafka | This library |
| :--- | :--- |
| `@EnableKafka` | `@EnableSolace` |
| `@KafkaListener` | `@SolaceListener` |
| `KafkaTemplate` | `SolaceTemplate` |
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` |
| `ProducerFactory` / `ConsumerFactory` | `SolaceSessionFactory` |
| `ConcurrentKafkaListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` |
| `KafkaListenerEndpointRegistry` | `SolaceListenerEndpointRegistry` |
| `KafkaTransactionManager` | `SolaceTransactionManager` |
| `ConsumerRecord` | `SolaceRecord` |

Plus what Solace makes possible and Kafka does not:

* **All three [exchange patterns](docs/exchange-patterns.md)** — publish-subscribe, point-to-point and
  request-reply — declared as `@SolaceListener(pattern = ...)` rather than assembled from flags.
* **[Local transactions](docs/transactions.md)** driven by `@Transactional` or a
  `TransactionTemplate`, so consuming a request and publishing its reply commit as one unit.
* **Per-instance reply destinations**, so request-reply works across a scaled deployment with no
  broker-side selector.
* **Three endpoint modes** — durable queue, temporary queue, direct topic subscription.

---

## Documentation

### Concepts

| Document | Contents |
| :--- | :--- |
| [Architecture](docs/architecture.md) | Layering, startup sequence, message flow, threading model, connection budget, lifecycle phases |
| [Exchange patterns](docs/exchange-patterns.md) | Publish-subscribe, point-to-point, request-reply, and the wiring each implies |
| [Transactions](docs/transactions.md) | What a commit covers, atomic consume-and-reply, poison messages, the transaction API |
| [Configuration reference](docs/configuration.md) | Every property, type and default |
| [Troubleshooting](docs/troubleshooting.md) | Broker errors and startup failures, what each means, and the fix |

### Class reference

| Package | Document | Classes |
| :--- | :--- | :--- |
| `…solace.annotation` | [annotations.md](docs/annotations.md) | `@EnableSolace`, `@SolaceListener` |
| `…solace.core` | [core.md](docs/core.md) | `SolaceSessionFactory`, `DefaultSolaceSessionFactory`, `SolaceOperations`, `SolaceTemplate`, `SolaceMessageConverter`, `JacksonSolaceMessageConverter`, `SolaceHeaderMapper`, `DefaultSolaceHeaderMapper`, `SolaceHeaders`, `SolaceRecord`, `EndpointMode`, `ExchangePattern`, `SolaceMessagingException` |
| `…solace.listener` | [listener.md](docs/listener.md) | `SolaceMessageListener`, `SolaceListenerErrorHandler`, `SolaceMessageListenerContainer`, `DefaultSolaceMessageListenerContainer`, `SolaceListenerEndpoint`, `ContainerProperties`, `ContainerKeepAlive`, `AbstractSolaceListenerAdapter`, `MethodSolaceListenerAdapter`, `RecordSolaceListenerAdapter`, `SolaceListenerContainerFactory`, `DefaultSolaceListenerContainerFactory`, `SolaceListenerEndpointRegistry`, `SolaceListenerAnnotationBeanPostProcessor`, `SolaceListenerConfigUtils` |
| `…solace.requestreply` | [request-reply.md](docs/request-reply.md) | `ReplyingSolaceTemplate`, `RequestReplyFuture`, `SolaceReplyTimeoutException` |
| `…solace.transaction` | [transactions.md](docs/transactions.md#class-reference) | `SolaceTransactionManager`, `SolaceResourceHolder`, `SolaceTransactionUtils` |
| `…solace.support` | [support.md](docs/support.md) | `InstanceIdProvider`, `HostnameInstanceIdProvider`, `ReplyDestinationResolver` |
| `cris.prs.solace.autoconfigure` | [autoconfiguration.md](docs/autoconfiguration.md) | `SolaceAutoConfiguration`, `SolaceAnnotationDrivenConfiguration`, `SolaceBootstrapConfiguration`, `SolaceProperties` |

---

## Getting started

### Dependency

```groovy
dependencies {
    implementation project(":solace-library")
}
```

The module brings `solace-java-spring-boot-starter`, `spring-messaging`, `spring-tx` and
`jackson-databind` transitively.

### Minimum configuration

```yaml
solace:
  java:
    host: tcp://broker:55555
    msgVpn: default
    clientUsername: app
    clientPassword: secret
```

Everything else has a default — see the [configuration reference](docs/configuration.md).

### Publish

```java
@Autowired SolaceTemplate<Object> solace;

solace.send("events/created", event);                              // topic
solace.send("events/created", correlationId, event);               // with a correlation id
solace.send("events/created", event, Map.of("tenant", "north"));   // extra SDT properties
solace.send("queue:audit", event);                                 // straight to a queue
```

### Consume

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", topics = "events/created", queue = "events")
public void onEvent(Event event) { }
```

Methods may also take `Message<T>`, `SolaceRecord<T>`, the raw `BytesXMLMessage`, and
`@Header` / `@Headers` parameters. A non-void return value is published to the request's `replyTo`.

### Request-reply

```java
@Autowired ReplyingSolaceTemplate solace;

RequestReplyFuture<Ack> future = solace.sendAndReceive("orders/place", order, Ack.class);
Ack ack = future.get();
long latency = future.getLatency();
```

Replies arrive on `<reply-topic-prefix>/<instance-id>`, unique to this pod, so several replicas can
issue requests concurrently without a broker-side filter.

---

## Design notes

Three decisions worth knowing before extending the library:

**The exchange pattern is a policy, not a flag.** Fan-out and competing consumers differ only in
whether each instance binds its own endpoint or they all share one. Expressed as three independent
settings, the wrong combinations outnumber the right ones — and a wrong one fails silently by
duplicating work or dropping most of it. `pattern` names the intent once and fills in the rest.

**A commit covers the whole session.** A Solace transacted session's `commit()` acknowledges every
message delivered on that session, not just the one in hand. That is why transacted flows are driven
by the thread their messages arrive on, why `EXECUTOR` dispatch is refused for them, and why each
transactional container gets its own connection.

**Every JCSMP thread is a daemon thread.** A consumer-only application would otherwise start
perfectly and exit immediately. The library holds the process open itself rather than relying on a
web server being present.

---

## Requirements

| | |
| :--- | :--- |
| Java | 24 (source and target level of this build) |
| Spring Boot | 3.5.x |
| Solace JCSMP | 10.27.x, via `solace-spring-boot-bom` 2.5.0 |
