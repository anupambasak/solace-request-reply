# solace-library

**A Spring integration library for Solace PubSub+, built on JCSMP.**

The programming model Spring for Apache Kafka gives a Kafka application, for Solace: a template for
sending, an annotation for receiving, a container that manages connections and threading, a
`PlatformTransactionManager`, and auto-configuration that wires it all from `application.yaml`.

No Spring Cloud Stream, no binder, no JMS.

---

## Quick look

```yaml
solace:
  java:
    host: tcp://localhost:55555
    msg-vpn: default
    client-username: default
    client-password: default
```

```java
// send
@Autowired SolaceTemplate<Object> solace;
solace.send("orders/created", order);

// receive — one instance in the group gets each message
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers",
        topics = "orders/created", concurrency = "5")
public void onOrder(Order order) { … }

// receive — every instance gets a copy
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "audit", topics = "orders/created")
public void onOrderForAudit(Order order) { … }

// respond — returning a value is the whole opt-in
@SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", group = "v1", topics = "pricing/quote")
public Quote quote(PriceRequest request) { return new Quote(...); }

// request
@Autowired ReplyingSolaceTemplate replying;
RequestReplyFuture<Quote> future = replying.sendAndReceive("pricing/quote", request, Quote.class);
Mono.fromFuture(future).subscribe(...);
```

No `@EnableSolace` step: auto-configuration turns annotation-driven listeners on, exactly as Spring
Boot does for Kafka.

---

## Documentation

The full guides live in [`docs/`](docs/00-index.md), numbered so reading order is learning order and
grouped into four parts. The **[documentation index](docs/00-index.md)** is the complete table of
contents, with a reading map.

**Start here:** [1. Introduction](docs/01-introduction.md)  ·  [2. Quickstart](docs/02-quickstart.md)  ·  [3. Architecture](docs/03-architecture.md)  ·  [4. Modules & reference app](docs/04-modules.md)

**Jump to:** [5. Exchange patterns](docs/05-exchange-patterns.md)  ·  [8. Request-reply](docs/08-request-reply.md)  ·  [13. Spring integration](docs/13-spring-integration.md)  ·  [15. Configuration](docs/15-configuration.md)  ·  [21. Troubleshooting](docs/21-troubleshooting.md)  —  or the full [index](docs/00-index.md).

**Reference-app module guides:** [`client/README.md`](../client/README.md) (requester)  ·  [`server/README.md`](../server/README.md) (responder).

---

## Package layout

```
org.cris.prs.messaging.solace
 ├── core/           sessions, SolaceTemplate, converters, headers, records, enums
 ├── listener/       containers, factory, registry, adapters, the annotation post-processor
 ├── requestreply/   ReplyingSolaceTemplate, ReplyEndpointSpec, the factory, futures
 ├── transaction/    SolaceTransactionManager, resource holder, utils
 ├── support/        InstanceIdProvider, ReplyDestinationResolver
 ├── schema/         optional schema registry converter: Avro, Protobuf, JSON Schema via Apicurio
 └── annotation/     @EnableSolace, @SolaceListener

org.cris.prs.solace.autoconfigure     ← deliberately OUTSIDE org.cris.prs.messaging
 └── SolaceAutoConfiguration, SolaceProperties, bootstrap and annotation-driven config
```

The auto-configuration package is separate on purpose. A component-scanned `@AutoConfiguration` class
is evaluated too early — before the Solace starter has contributed `SpringJCSMPFactory` — and every
bean silently disappears. [13.9](docs/13-spring-integration.md#139-why-the-auto-configuration-package-is-separate)
explains it in full.

---

## The Spring for Kafka mapping

| Spring for Apache Kafka | This library |
| :--- | :--- |
| `KafkaTemplate` | `SolaceTemplate` |
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` |
| `@KafkaListener` | `@SolaceListener` |
| `@EnableKafka` | `@EnableSolace` |
| `KafkaListenerEndpointRegistry` | `SolaceListenerEndpointRegistry` |
| `ConcurrentMessageListenerContainer` | `DefaultSolaceMessageListenerContainer` |
| `KafkaTransactionManager` | `SolaceTransactionManager` |
| `ConsumerRecord` | `SolaceRecord` |
| `spring.kafka.*` | `solace.*` |

Full table, and where the two genuinely differ, in [1.2](docs/01-introduction.md#12-the-spring-for-kafka-mapping).

---

## Building

```bash
gradle :solace-library:compileJava
gradle :solace-library:test
gradle :solace-library:javadoc        # -Xdoclint:all; a broken reference fails the build
```

Javadoc lands in `build/docs/javadoc/index.html`. One editing rule: **never `{@link}` a
Lombok-generated accessor** — Lombok generates after javadoc reads the source, so the reference cannot
be resolved and doclint reports it as an error. Use `{@code …}`.

---

## Requirements

Java 21+ · Spring Boot 3.5.x · Solace JCSMP 10.27.x via `solace-java-spring-boot-starter` · Jackson ·
optionally Apicurio Registry serdes 3.3.x (Avro, Protobuf, JSON Schema)
