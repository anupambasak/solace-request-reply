# Feature backlog

An assessment of what the [Solace platform](https://docs.solace.com/Get-Started/solace-platform.htm)
offers against what `solace-library` implements today.

**Method.** The platform page describes capabilities at product level — data liberation, real-time
streaming and filtering, reactive processing, data democratization, plus the event mesh and the
platform services. Those only become library features where the
[JCSMP feature matrix](https://docs.solace.com/API/API-Developer-Guide/Feature-Support-PubSub-Messaging-APIs.htm)
exposes an API for them, so every item below is checked against that matrix. Anything the matrix
marks unsupported for JCSMP, or that lives outside the client API entirely, is listed in
[Not library features](#not-library-features) rather than quietly omitted.

---

## Already implemented

| Solace capability | Where |
| :--- | :--- |
| Direct and guaranteed messaging | `EndpointMode.DIRECT` / `DURABLE_QUEUE` / `NON_DURABLE_QUEUE` |
| Publish-subscribe, point-to-point, request-reply | `ExchangePattern`, `@SolaceListener(pattern = …)` |
| Local transactions | `SolaceTransactionManager` |
| Durable endpoint provisioning | `ContainerProperties.Endpoint`, `provisionEndpoint` |
| Selectors | `@SolaceListener(selector = …)` |
| Smart topic hierarchy and wildcards | topic subscriptions, per-instance topic levels |
| Dead message queue and max-redelivery | `ContainerProperties.Endpoint.DeadMessageQueue` |
| Client acknowledgement | `ackOnError`, transactional commit |
| Structured data types (partially) | headers → SDT user properties |

---

## Tier 1 — closes a real gap in what is already built

### 1. Negative acknowledgement and settlement outcomes

*JCSMP 10.17+. `XMLMessage.settle(Outcome)`, `ConsumerFlowProperties.addRequiredSettlementOutcomes(…)`.*

Today a failing listener has two outcomes, and both are unsatisfying: `ackOnError: true` **discards**
the message, and `ackOnError: false` leaves it unacknowledged until the flow is rebound. Negative
acknowledgement adds the two answers that are actually wanted — `Outcome.FAILED` returns the message
for redelivery and increments its redelivery count, and `Outcome.REJECTED` sends it straight to the
dead message queue without burning through `max-redelivery-count` first.

This is the single most valuable addition, because it completes error handling the library already
half-implements: a poison message could be rejected on the first attempt rather than after five
pointless retries, and a transient failure could be returned for redelivery without the rollback
machinery a transaction requires.

**Shape:** an `ErrorHandlingDecision` returned by `SolaceListenerErrorHandler`, or
`ContainerProperties.errorOutcome` (`ACK` / `FAILED` / `REJECTED`). Flows must declare the outcomes
they will use at bind time. **Effort: S.**

### 2. Consumer flow tuning

*`setTransportWindowSize`, `setAckThreshold`, `setAckTimerInMsecs`, `setReconnectTries`,
`setReconnectRetryIntervalInMsecs`.*

The library builds `ConsumerFlowProperties` but exposes only endpoint, selector and ack mode. The
transport window is the primary throughput knob for guaranteed messaging, and the ack threshold and
timer trade acknowledgement round-trips against redelivery risk. Right now the only way to tune a
flow is to fork the container.

**Shape:** a `ContainerProperties.Flow` block mirroring the existing `Endpoint` block. Purely
additive, no behaviour change at defaults. **Effort: S.**

### 3. Flow event handling

*`ConsumerFlowProperties.setActiveFlowIndication(boolean)`, `FlowEventHandler`,
`JCSMPSession.createFlow(listener, flowProps, endpointProps, flowEventHandler)`.*

The container passes no `FlowEventHandler`, so flow-level events — bind failures, reconnects, flow
down, and active-flow indication on exclusive endpoints — are invisible. An application scaled across
pods on an exclusive endpoint currently has no way to learn which instance is the active consumer.

Two features fall out of one change: operational visibility of reconnects, and an
`isActive()`/`ActiveFlowListener` for leader-style consumers.

**Shape:** an optional `SolaceFlowListener` on the container, plus lifecycle logging by default.
**Effort: S.**

### 4. Delivery count on the received message

*`XMLMessage.getDeliveryCount()`, `isDeliveryCountSupported()`.*

`SolaceRecord.isRedelivered()` is a boolean — a handler can tell that a message was redelivered but
not how many times, so "log at warn on the third attempt, reject on the fifth" is not expressible.
The broker already tracks the count.

**Shape:** `SolaceRecord.getDeliveryCount()`, and a `@Header` constant. **Effort: XS.**

### 5. Queue browsing

*`JCSMPSession.createBrowser(BrowserProperties)`.*

The library provisions a dead message queue but offers no way to look inside it. Browsing reads
messages from a queue **without consuming them**, which is exactly what an operator needs for a DMQ,
and what an admin endpoint needs for queue depth and inspection.

**Shape:** a `SolaceBrowser` / `SolaceOperations.browse(queue, limit)` returning a stream of
`SolaceRecord`. Natural companion to the DMQ support already present. **Effort: M.**

---

## Tier 2 — new capability, larger surface

### 6. Partitioned queues

*JCSMP 10.19+.*

`POINT_TO_POINT` today gives competing consumers with **no ordering guarantee** — two messages about
the same entity can be processed concurrently by different workers. Partitioned queues fix that: the
broker hashes a partition key and pins every message with the same key to one consumer, giving
Kafka-style per-key ordering while still scaling out.

For a library modelled on Spring for Apache Kafka this is arguably the biggest conceptual gap: Kafka
users expect per-key ordering, and today's point-to-point silently does not provide it.

**Shape:** a `partitionKey` on send (a message property), a partition count on endpoint provisioning,
and documentation of the ordering guarantee in `exchange-patterns.md`. **Effort: M.**

### 7. Message replay

*JCSMP 10.11+. `ConsumerFlowProperties.setReplayStartLocation(ReplayStartLocation)`,
`JCSMPFactory.createReplayStartLocationBeginning()` / `createReplayStartLocationDate(Date)`.*

Replay re-delivers messages the broker has already spooled, from the beginning of the replay log or
from a timestamp. It turns the broker into a short-term event store: rebuild a projection after a
bug, or bring a new service online with history rather than only new events.

Needs care in the container: a replay flow is a distinct mode, replay failures arrive as flow events,
and replay on a shared endpoint affects every consumer of it.

**Shape:** `@SolaceListener(replayFrom = "BEGINNING" | ISO-8601)`, or a runtime operation on a
container obtained from the registry — the latter is more useful, since replay is an operational act
rather than a deployment-time setting. Depends on flow event handling (#3). **Effort: M.**

### 8. Distributed tracing

*JCSMP 10.17+ manual, 10.26+ auto-instrumentation; `solace-opentelemetry-jcsmp-integration`.*

Called out on the platform page: tracking a message from the sending application, between brokers, to
the receiving one. The library has exactly the shape that benefits — a request-reply hop where the
correlation id already exists but carries no trace context, so a request and its reply appear as two
unrelated spans.

**Shape:** propagate the OpenTelemetry context into message properties on send and extract it into
the listener's scope, behind an optional dependency so tracing stays opt-in. **Effort: M.**

### 9. PubSub+ Cache

*`JCSMPSession.createCacheSession(…)`, supported by JCSMP per the matrix.*

Solves the late-joiner problem for publish-subscribe: a subscriber starting now sees only future
messages. A cache request returns the last value, or a window of history, for a topic — so a new
instance can initialise its state from the topic rather than from a database.

**Shape:** a `SolaceCacheTemplate` with `requestLatest(topic)` / `requestHistory(topic, count)`, and
optionally an `@SolaceListener(cacheOnStart = true)` that primes a listener before live delivery.
Requires a PubSub+ Cache deployment, so it must degrade cleanly when absent. **Effort: L.**

### 10. Topic dispatch

*Listed as supported for JCSMP.*

One flow or session, with per-topic listeners registered against it. Today each `@SolaceListener`
gets its own endpoint and flows; a service subscribing to twenty related topics pays for twenty
endpoints. Topic dispatch lets one endpoint fan out to different handler methods by topic.

**Shape:** several `@SolaceListener` methods sharing a `queue`, with the container dispatching by
matched subscription. A meaningful change to the container's dispatch path. **Effort: L.**

### 11. Structured Data Types as a payload format

*Supported; the library uses SDT only for headers.*

A `SdtSolaceMessageConverter` writing `SDTMap`/`SDTStream` bodies would let this library interoperate
with non-Java Solace applications that expect SDT rather than JSON — the usual case when talking to
existing C, .NET or JMS estates.

**Shape:** a second `SolaceMessageConverter`. Self-contained, no container changes. **Effort: S.**

### 12. `noLocal`

*`ConsumerFlowProperties.setNoLocal(boolean)`.*

Suppresses delivery of messages the same session published. Directly useful for publish-subscribe,
where a service that both publishes and subscribes to a topic currently receives its own broadcasts
and has to filter them out by instance id.

**Shape:** `@SolaceListener(noLocal = "true")`. **Effort: XS.**

---

## Tier 3 — configuration passthrough and documentation

These are supported by the JCSMP API and already reachable through
`solace.java.apiProperties.*` on the starter. What is missing is first-class properties and
documentation, not capability.

| Feature | Note |
| :--- | :--- |
| **OAuth 2.0 authentication and token refresh** (10.13+/10.16+) | The starter exposes `solace.java.oauth2ClientRegistrationId`; the library never mentions it. Documentation-only. **XS** |
| **Kerberos authentication** | Config passthrough. **XS** |
| **Compression** — streaming, and end-to-end payload compression (10.24+) | `COMPRESSION_LEVEL` API property; worth a named property and a note on the throughput trade-off. **XS** |
| **Proxy connections** (HTTP, SOCKS5) | Config passthrough for restricted networks. **XS** |
| **Durable endpoint deprovisioning** | The library provisions but never removes. A `deprovisionOnStop` flag would help ephemeral test environments; dangerous as a default. **S** |
| **On-behalf-of subscription manager** | Lets one client manage another's subscriptions. Niche, but the only way to pre-subscribe an endpoint for a client that has not connected yet. **M** |
| **Solace Schema Registry SERDES** (10.28+) | Another `SolaceMessageConverter`, with schema validation and evolution. **M** |

---

## Beyond the platform page: completing the Spring analogy

Not Solace features, but gaps against the Spring for Apache Kafka model the library sets out to
match. Listed because they are what a user of that model will look for next.

| | Why |
| :--- | :--- |
| **Batch listeners** — `List<T>` payloads | Kafka's `batchListener`. Amortises per-message overhead for high-volume consumers. **M** |
| **Retry and backoff** | Spring Kafka's `DefaultErrorHandler` with `BackOff`. Today a failure is retried by the broker immediately, with no delay and no attempt limit short of the DMQ. **M** |
| **Micrometer metrics** | Publish counts, latencies, flow state, `ReplyingSolaceTemplate.getPendingCount()`. All the data exists; nothing is exported. **S** |
| **Actuator health indicator** | Session connectivity and container state as a readiness signal. Directly useful for the Kubernetes deployment already in the repo. **S** |
| **Test support** | A `@EmbeddedSolace`-style Testcontainers rule. Today the tests can only cover logic that needs no broker. **M** |
| **Record filter strategy** | Discard uninteresting messages before conversion, as Kafka's `RecordFilterStrategy` does. **S** |

---

## Not library features

Stated explicitly so the list is honest about scope.

| Platform capability | Why it is not a library feature |
| :--- | :--- |
| **Event Portal** — event design, cataloguing, discovery, governance | A design-time and governance product. The library could *contribute* to it by exporting AsyncAPI from `@SolaceListener` metadata, which would be a genuine feature — but the portal itself is not one. |
| **Solace Insights** — infrastructure monitoring | An operator-facing service over broker telemetry, not the client API. |
| **Event mesh** — federation, cross-environment streaming, dynamic message routing | Broker-to-broker configuration. Applications benefit transparently; there is nothing to implement client-side. |
| **Micro-integrations, Solace Agent Mesh** | Separate runtime products. |
| **XA transactions** | The feature matrix marks these **unsupported for JCSMP**. Only local transactions are available, which is what the library implements. |

---

## Suggested order

1. **Negative acknowledgement** (#1) and **delivery count** (#4) — small, and they finish the error
   handling already in place.
2. **Flow event handling** (#3) and **flow tuning** (#2) — small, and #3 unblocks replay.
3. **Partitioned queues** (#6) — the largest conceptual gap against the Kafka model this library
   imitates.
4. **Queue browsing** (#5) — makes the dead message queue support usable in practice.
5. **Distributed tracing** (#8) — the platform capability that most rewards the request-reply shape.

Items 1–4 of that order are all small or medium and together remove every "you cannot express that"
answer in the current error-handling and consumer-tuning story.
