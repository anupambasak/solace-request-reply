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

Read in order for a full picture, or jump to what you need.

### Start here

| | |
| :--- | :--- |
| **[1. Overview](docs/01-overview.md)** | What it is, why it exists, the Spring-for-Kafka mapping, the Solace concepts you need, and what it deliberately does not do |
| **[2. Getting started](docs/02-getting-started.md)** | Dependency, configuration, and complete working examples of all three patterns plus transactions |
| **[3. Architecture](docs/03-architecture.md)** | Layers, the runtime object graph, startup and shutdown sequences, the send and receive paths, the threading model, session strategy |

### Spring and configuration

| | |
| :--- | :--- |
| **[4. Spring integration](docs/04-spring-integration.md)** | Every framework contract the library implements: auto-configuration and its conditions, property binding, the `BeanPostProcessor`, listener method signatures, lifecycle phases, the transaction manager, and how to override any of it |
| **[5. Configuration reference](docs/05-configuration.md)** | Every property, type, default and effect — plus precedence rules and environment-specific recipes |
| **[6. Annotations](docs/06-annotations.md)** | `@EnableSolace` and `@SolaceListener`, attribute by attribute, with worked declarations |

### Using it

| | |
| :--- | :--- |
| **[7. Exchange patterns](docs/07-exchange-patterns.md)** | Publish-subscribe, point-to-point and request-reply: what each one wires up, and how to choose |
| **[8. Producing messages](docs/08-producing-messages.md)** | `SolaceTemplate`, delivery defaults, headers, and the single/multiple/batch distinction |
| **[9. Consuming messages](docs/09-consuming-messages.md)** | Containers, endpoint naming, provisioning, concurrency, dispatch modes, acknowledgement, redelivery and the DMQ |
| **[10. Request-reply](docs/10-request-reply.md)** | Correlation, per-instance reply destinations, timeouts, futures, and when to give a service its own reply endpoint |
| **[11. Transactions](docs/11-transactions.md)** | Solace local transactions through `@Transactional` and `TransactionTemplate`, the transacted-session budget, and the database interaction |

### Reference

| | |
| :--- | :--- |
| **[12. Conversion and headers](docs/12-conversion-and-headers.md)** | The converter and header-mapper SPIs, `SolaceHeaders`, `SolaceRecord`, and where the message body actually lives |
| **[13. Multi-instance](docs/13-multi-instance.md)** | Instance ids, destination naming, and what changes when you scale |
| **[14. Extension points](docs/14-extension-points.md)** | Every replaceable collaborator, with examples |
| **[15. Class reference](docs/15-class-reference.md)** | Every public type, one table per package |
| **[16. Operations](docs/16-operations.md)** | Logging, what to monitor, sizing, deployment, and a pre-flight checklist |
| **[17. Troubleshooting](docs/17-troubleshooting.md)** | Symptom → cause → fix, for everything that has actually gone wrong |
| **[18. Feature backlog](docs/18-feature-backlog.md)** | Solace platform capabilities assessed against what is implemented |

---

## Package layout

```
cris.prs.messaging.solace
 ├── core/           sessions, SolaceTemplate, converters, headers, records, enums
 ├── listener/       containers, factory, registry, adapters, the annotation post-processor
 ├── requestreply/   ReplyingSolaceTemplate, ReplyEndpointSpec, the factory, futures
 ├── transaction/    SolaceTransactionManager, resource holder, utils
 ├── support/        InstanceIdProvider, ReplyDestinationResolver
 └── annotation/     @EnableSolace, @SolaceListener

cris.prs.solace.autoconfigure     ← deliberately OUTSIDE cris.prs.messaging
 └── SolaceAutoConfiguration, SolaceProperties, bootstrap and annotation-driven config
```

The auto-configuration package is separate on purpose. A component-scanned `@AutoConfiguration` class
is evaluated too early — before the Solace starter has contributed `SpringJCSMPFactory` — and every
bean silently disappears. [4.9](docs/04-spring-integration.md#49-why-the-auto-configuration-package-is-separate)
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

Full table, and where the two genuinely differ, in [1.2](docs/01-overview.md#12-the-spring-for-kafka-mapping).

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

Java 21+ · Spring Boot 3.5.x · Solace JCSMP 10.27.x via `solace-java-spring-boot-starter` · Jackson
