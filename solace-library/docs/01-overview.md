# 1. Overview

`solace-library` is a Spring integration library for **Solace PubSub+**, built directly on the
**JCSMP** API. It gives a Spring Boot application the same programming model that Spring for Apache
Kafka gives a Kafka application: a template for sending, an annotation for receiving, a container
that manages the connection and threading, a `PlatformTransactionManager`, and auto-configuration
that wires all of it from `application.yaml`.

It has no dependency on Spring Cloud Stream, on the Solace binder, or on JMS.

---

## 1.1 Why it exists

Solace's own Spring support stops at the connection. `solace-java-spring-boot-starter` gives you a
configured `SpringJCSMPFactory` and nothing above it, so an application ends up hand-writing session
management, flow binding, endpoint provisioning, message conversion, correlation of replies, and
transaction handling — the same few hundred lines in every service, each time slightly differently.

The alternative, Spring Cloud Stream's Solace binder, solves that by hiding Solace almost entirely
behind a generic binding abstraction. That works until you need something Solace-specific: a
selector, a non-exclusive queue with a specific access type, a per-instance temporary endpoint, a
local transaction spanning a consume and a publish.

This library sits between the two. It is Solace-shaped — endpoints, flows, transacted sessions and
delivery modes are all first-class and named as Solace names them — while the Spring-facing surface
is deliberately identical in shape to Spring for Kafka, so the concepts transfer.

---

## 1.2 The Spring for Kafka mapping

If you know Spring for Kafka, you already know this library's surface. The correspondence is exact
by design, and it is the fastest way to orient yourself.

| Spring for Apache Kafka | This library | Notes |
| :--- | :--- | :--- |
| `KafkaTemplate<K,V>` | `SolaceTemplate<T>` | Send, with configurable defaults |
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` | Request-reply with correlated futures |
| `RequestReplyFuture` | `RequestReplyFuture<R>` | Extends `CompletableFuture`; also carries latency |
| `@KafkaListener` | `@SolaceListener` | Method-level listener declaration |
| `@EnableKafka` | `@EnableSolace` | Rarely needed — auto-configuration turns it on |
| `KafkaListenerAnnotationBeanPostProcessor` | `SolaceListenerAnnotationBeanPostProcessor` | Finds annotated methods, builds endpoints |
| `KafkaListenerEndpointRegistry` | `SolaceListenerEndpointRegistry` | Holds and lifecycles the containers |
| `ConcurrentKafkaListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | Builds a container from an endpoint |
| `ConcurrentMessageListenerContainer` | `DefaultSolaceMessageListenerContainer` | Binds flows, dispatches messages |
| `ContainerProperties` | `ContainerProperties` | Same name, same role |
| `KafkaTransactionManager` | `SolaceTransactionManager` | Local transactions via `@Transactional` |
| `RecordMessageConverter` | `SolaceMessageConverter` | Body conversion SPI |
| `KafkaHeaderMapper` | `SolaceHeaderMapper` | Header conversion SPI |
| `ConsumerRecord` | `SolaceRecord<T>` | Payload plus message metadata |
| `KafkaAutoConfiguration` | `SolaceAutoConfiguration` | Same conditions-and-defaults approach |
| `spring.kafka.*` | `solace.*` | Same `@ConfigurationProperties` binding |

Where the two diverge, it is because Solace differs from Kafka, not because of a design preference:

- **No partitions, no offsets.** Ordering and parallelism come from flows on an endpoint, not from
  partition assignment. `concurrency` means "bind this many flows", not "assign this many partitions".
- **No consumer-group protocol.** A "group" here is a naming convention (`<queue>.<group>`) that
  produces a single shared non-exclusive endpoint. There is no rebalance, no generation, no
  coordinator. What Kafka gets from partition assignment, Solace gets from an *exclusive* endpoint:
  the broker names one active consumer and tells it so, which is leader election without a
  coordinator — see [9.8](09-consuming-messages.md#98-flow-events).
- **Topics are not endpoints.** In Solace a topic is a routing key on a published message. A consumer
  binds to an *endpoint* (a queue) and attaches topic subscriptions to it. Almost every Solace-specific
  concept in this library follows from that one fact.
- **Local transactions only.** JCSMP supports transacted sessions, not XA. There is no
  exactly-once producer/consumer chain across brokers.

---

## 1.3 Solace concepts you need

The library does not hide these, so it is worth being precise about them.

| Term | Meaning |
| :--- | :--- |
| **Session** | A `JCSMPSession` — one TCP connection to the broker, authenticated to a Message VPN. Sessions are expensive; the library shares one by default. |
| **Message VPN** | A virtual broker: its own namespace of endpoints, topics, clients and quotas. |
| **Topic** | A hierarchical routing key on a *published* message, e.g. `orders/eu/created`. Not a stored object; publishing to a topic nobody subscribes to discards the message. |
| **Topic subscription** | A pattern matched against published topics. `*` matches one level, `>` matches one or more trailing levels. Attached either to a session (direct) or to a queue (guaranteed). |
| **Endpoint / Queue** | A broker-side object that *stores* messages. A consumer binds a flow to it. Topic subscriptions on a queue cause matching published messages to be spooled into it. |
| **Durable queue** | Provisioned, named, survives restarts, keeps its subscriptions and its spooled messages. Shared by any client that binds to it. |
| **Non-durable / temporary queue** | Created when a client binds, deleted when it disconnects. Named `#P2P/QTMP/...`. Belongs to that one client and **accepts exactly one flow**. |
| **Flow** | A consumer's binding to an endpoint. `concurrency` in this library is the number of flows. |
| **Direct messaging** | At-most-once, no spooling, no acknowledgement. Subscriptions live on the session. Fastest, lossy on disconnect. |
| **Guaranteed messaging** | Persisted to the endpoint, acknowledged by the consumer, redelivered until acked. What `DeliveryMode.PERSISTENT` produces. |
| **Access type** | `EXCLUSIVE` (one active consumer, others standby) or `NONEXCLUSIVE` (competing consumers). A property of the endpoint, set at provision time. |
| **Transacted session** | A JCSMP local transaction scope. Consumes and publishes on it commit or roll back together. Capped per connection (10 by default). |
| **DMQ** | Dead Message Queue. Where a message goes after exceeding `max-redelivery-count`, if it is DMQ-eligible. |
| **Selector** | A JMS-style SQL92 predicate over message properties, evaluated broker-side, filtering what a flow receives. |

---

## 1.4 What the library does for you

1. **Connection and session management** — one shared session, extra sessions where the broker's
   per-connection limits require them, producers cached per session, everything closed on shutdown.
2. **Endpoint provisioning** — creates durable queues and the DMQ, attaches subscriptions, and
   tolerates the "already exists" cases that are normal on restart.
3. **Error handling** — settlement outcomes (`ACCEPTED` / `FAILED` / `REJECTED` / `NONE`) per
   container or per failure, delivery counts, redelivery limits and the dead message queue.
4. **Flow lifecycle** — reconnects, lost binds and active-consumer changes surfaced as events, with
   per-flow tuning of the transport window and acknowledgement behaviour.
5. **Threading and dispatch** — inline on the JCSMP delivery thread, or handed to a Spring
   `AsyncTaskExecutor` with bounded back-pressure.
6. **Message conversion** — Jackson by default, over a two-interface SPI you can replace.
7. **Header mapping** — Spring `MessageHeaders` ↔ Solace properties, including the standard fields.
8. **Request-reply correlation** — per-instance reply destinations, correlation ids, timeouts,
   futures, and latency measurement.
9. **Transactions** — a real `PlatformTransactionManager`, so `@Transactional` and
   `TransactionTemplate` work.
10. **Multi-instance safety** — every per-instance destination carries a sanitised pod/host id.
11. **Lifecycle** — `SmartLifecycle` phases ordered so containers are consuming before the
   request-reply template can send, and a non-daemon keep-alive thread so a listener-only app does
   not exit.
12. **Observability** — Micrometer meters for listener throughput, latency, container state and
    request-reply traffic, plus an Actuator health indicator at `/actuator/health/solace`. Both are
    optional and both disappear cleanly when their dependency is absent.

## 1.5 What it deliberately does not do

- **XA / distributed transactions.** JCSMP does not offer them.
- **Batch listeners.** One message per invocation. (See [18. Feature backlog](18-feature-backlog.md).)
- **Schema registry, Avro, Protobuf.** The converter SPI is the extension point.
- **Broker administration.** It provisions the endpoints it needs and nothing else; use SEMP or the
  admin UI for the rest.
- **Its own retry policy with back-off.** Redelivery is the broker's, governed by settlement
  outcomes, `max-redelivery-count` and the DMQ. There is no in-container delay or exponential
  back-off.

---

## 1.6 Requirements

| | |
| :--- | :--- |
| Java | 21+ (the reference application builds on 24) |
| Spring Boot | 3.5.x |
| Solace JCSMP | 10.27.x, via `com.solace.spring.boot:solace-java-spring-boot-starter` |
| Jackson | `jackson-databind`, for the default converter |

---

**Next:** [2. Getting started](02-getting-started.md)
