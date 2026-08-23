# 18. Feature backlog

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
| Client acknowledgement | `errorOutcome`, transactional commit |
| Negative acknowledgement / settlement outcomes | `SettlementOutcome`, `SolaceListenerErrorHandler.resolveOutcome` |
| Delivery count | `SolaceRecord.getDeliveryCount()`, `SolaceHeaders.DELIVERY_COUNT` |
| Structured data types (partially) | headers → SDT user properties |
| Micrometer metrics | `SolaceListenerMetrics`, `SolaceRequestReplyMetrics`, the `observability` package |
| Actuator health indicator | `SolaceHealthIndicator`, `SolaceSessionFactory.isHealthy()` |

---

## Recently implemented

Both were Tier-"beyond the platform page" items; they are described here rather than only in the
table above because the design choices are worth recording.

### Micrometer metrics

Two dependency-free SPIs — `SolaceListenerMetrics` in the `listener` package and
`SolaceRequestReplyMetrics` in `requestreply` — are called on the message path by
`DefaultSolaceMessageListenerContainer` and `ReplyingSolaceTemplate`. Both default to a `NO_OP`
implementation, so instrumentation costs nothing when it is not wired up, and neither package gains a
dependency on a metrics library.

The Micrometer implementations live in a new `cris.prs.messaging.solace.observability` package and are
registered by `SolaceObservabilityConfiguration` when a `MeterRegistry` bean is present. Counters and
timers are recorded as messages flow; the state gauges are registered by `SolaceMetricsBinder`, a
`SmartLifecycle` in the highest phase — a Micrometer `MeterBinder` would have bound before the
listener containers were registered, which happens in the annotation post-processor's
`afterSingletonsInstantiated`.

Meters: `solace.listener.messages.received`, `solace.listener.processing`, `solace.listener.running`,
`solace.listener.flows`, `solace.requests.sent`, `solace.requests.send.failed`,
`solace.requests.latency`, `solace.requests.timeouts`, `solace.requests.pending`,
`solace.replies.unmatched`. See [16. Operations](16-operations.md#162-micrometer-metrics).

**Still open:** broker-side statistics (`JCSMPSession` exposes session stats that are not sampled),
and per-endpoint spool depth, which is only available through SEMP.

### Actuator health indicator

`SolaceHealthIndicator` contributes `/actuator/health/solace` from state already held in memory — it
never contacts the broker, so it is cheap enough for a readiness probe. It reports DOWN when the
session factory says its connection is gone, or when a registered container is not running.

That second rule is configurable (`solace.health.require-all-containers-running`) because a container
declared with `autoStartup = "false"`, or stopped deliberately through the registry, is not a fault —
and reporting it as one would keep the instance out of the load balancer indefinitely.

`SolaceSessionFactory` gained a `default boolean isHealthy()` for this. A default method rather than a
new abstract one, so a custom session factory keeps compiling and is simply reported as healthy.

**Still open:** distinguishing "connected" from "reconnecting". JCSMP reconnects transparently and the
library does not yet subscribe to session events, so a session in the middle of a reconnect still
reports connected. Flow event handling (Tier 1, item 2) is the prerequisite.

### Negative acknowledgement and settlement outcomes

*Was Tier 1, item 1. JCSMP 10.17+: `XMLMessage.settle(XMLMessage.Outcome)`,
`ConsumerFlowProperties.addRequiredSettlementOutcomes(XMLMessage.Outcome...)`. Note the enum is
**nested on `XMLMessage`**, not a top-level `com.solacesystems.jcsmp.Outcome`.*

`SettlementOutcome` — `ACCEPTED` / `FAILED` / `REJECTED` / `NONE` — replaces the boolean
`ackOnError`, which is deprecated but still honoured when `errorOutcome` is unset (`true` → `ACCEPTED`,
`false` → `NONE`). Configurable globally as `solace.listener.error-outcome` and per listener as
`@SolaceListener(errorOutcome = "…")`.

The backlog proposed *either* an `ErrorHandlingDecision` returned by the error handler *or* a
container property. Both were built, because they answer different questions: the property is the
policy for a listener, and `SolaceListenerErrorHandler.resolveOutcome` is the policy for a *failure*.
The right outcome usually depends on why it failed — reject something that will never deserialise,
retry a downstream timeout — which a single container-level setting cannot express. `resolveOutcome`
is a `default` returning `null`, so every existing lambda error handler keeps compiling and simply
defers to the container.

Bind-time negotiation is derived rather than configured: the container requests `FAILED` and
`REJECTED` when the resolved `errorOutcome` is one of them. `solace.listener.negative-acknowledgement`
forces it either way — `true` is required when an error handler decides per message, since the
container cannot know in advance what it will return, and `false` is the escape for a broker or client
too old to support settlement outcomes.

Ignored on transacted flows, where the rollback already governs redelivery; those flows do not
negotiate outcomes at all. A settle failure is logged with the property to set, not rethrown — the
JCSMP delivery thread can do nothing useful with it, and the broker resolves the state by redelivering.

**Still open:** `Outcome.ACCEPTED` on *success* is still sent as `ackMessage()`, which is the same
thing over the wire; there is no reason to change it, but a future settlement-only path would be
tidier.

### Delivery count on the received message

*Was Tier 1, item 4. `XMLMessage.getDeliveryCount()`, `isDeliveryCountSupported()`.*

`SolaceRecord.getDeliveryCount()`, `SolaceRecord.isDeliveryCountSupported()`, and the header
`SolaceHeaders.DELIVERY_COUNT` (`solace_deliveryCount`), so `@Header` works without taking a record.

The count is a broker feature negotiated per message, and `getDeliveryCount()` **throws**
`UnsupportedOperationException` where it is unavailable — so every read goes through
`DefaultSolaceHeaderMapper.deliveryCountOf`, which guards the capability check and the call and
degrades to `-1`. `isDeliveryCountSupported()` exists on the record because `-1` silently passes any
`>= n` comparison as a first delivery, which is the one way this feature can quietly mislead.

Together with settlement outcomes this makes a give-up policy expressible for the first time: return
`REJECTED` once the delivery count reaches three, `FAILED` before that. Neither half was enough on its
own — `isRedelivered()` could not count, and there was no way to reject early.

---

## Tier 1 — closes a real gap in what is already built

### 1. Consumer flow tuning

*`setTransportWindowSize`, `setAckThreshold`, `setAckTimerInMsecs`, `setReconnectTries`,
`setReconnectRetryIntervalInMsecs`.*

The library builds `ConsumerFlowProperties` but exposes only endpoint, selector and ack mode. The
transport window is the primary throughput knob for guaranteed messaging, and the ack threshold and
timer trade acknowledgement round-trips against redelivery risk. Right now the only way to tune a
flow is to fork the container.

**Shape:** a `ContainerProperties.Flow` block mirroring the existing `Endpoint` block. Purely
additive, no behaviour change at defaults. **Effort: S.**

### 2. Flow event handling

*`ConsumerFlowProperties.setActiveFlowIndication(boolean)`, `FlowEventHandler`,
`JCSMPSession.createFlow(listener, flowProps, endpointProps, flowEventHandler)`.*

The container passes no `FlowEventHandler`, so flow-level events — bind failures, reconnects, flow
down, and active-flow indication on exclusive endpoints — are invisible. An application scaled across
pods on an exclusive endpoint currently has no way to learn which instance is the active consumer.

Two features fall out of one change: operational visibility of reconnects, and an
`isActive()`/`ActiveFlowListener` for leader-style consumers.

**Shape:** an optional `SolaceFlowListener` on the container, plus lifecycle logging by default.
**Effort: S.**

### 3. Queue browsing

*`JCSMPSession.createBrowser(BrowserProperties)`.*

The library provisions a dead message queue but offers no way to look inside it. Browsing reads
messages from a queue **without consuming them**, which is exactly what an operator needs for a DMQ,
and what an admin endpoint needs for queue depth and inspection.

**Shape:** a `SolaceBrowser` / `SolaceOperations.browse(queue, limit)` returning a stream of
`SolaceRecord`. Natural companion to the DMQ support already present. **Effort: M.**

---

## Tier 2 — new capability, larger surface

### 4. Partitioned queues

*JCSMP 10.19+.*

`POINT_TO_POINT` today gives competing consumers with **no ordering guarantee** — two messages about
the same entity can be processed concurrently by different workers. Partitioned queues fix that: the
broker hashes a partition key and pins every message with the same key to one consumer, giving
Kafka-style per-key ordering while still scaling out.

For a library modelled on Spring for Apache Kafka this is arguably the biggest conceptual gap: Kafka
users expect per-key ordering, and today's point-to-point silently does not provide it.

**Shape:** a `partitionKey` on send (a message property), a partition count on endpoint provisioning,
and documentation of the ordering guarantee in `exchange-patterns.md`. **Effort: M.**

### 5. Message replay

*JCSMP 10.11+. `ConsumerFlowProperties.setReplayStartLocation(ReplayStartLocation)`,
`JCSMPFactory.createReplayStartLocationBeginning()` / `createReplayStartLocationDate(Date)`.*

Replay re-delivers messages the broker has already spooled, from the beginning of the replay log or
from a timestamp. It turns the broker into a short-term event store: rebuild a projection after a
bug, or bring a new service online with history rather than only new events.

Needs care in the container: a replay flow is a distinct mode, replay failures arrive as flow events,
and replay on a shared endpoint affects every consumer of it.

**Shape:** `@SolaceListener(replayFrom = "BEGINNING" | ISO-8601)`, or a runtime operation on a
container obtained from the registry — the latter is more useful, since replay is an operational act
rather than a deployment-time setting. Depends on flow event handling (#2). **Effort: M.**

### 6. Distributed tracing

*JCSMP 10.17+ manual, 10.26+ auto-instrumentation; `solace-opentelemetry-jcsmp-integration`.*

Called out on the platform page: tracking a message from the sending application, between brokers, to
the receiving one. The library has exactly the shape that benefits — a request-reply hop where the
correlation id already exists but carries no trace context, so a request and its reply appear as two
unrelated spans.

**Shape:** propagate the OpenTelemetry context into message properties on send and extract it into
the listener's scope, behind an optional dependency so tracing stays opt-in. **Effort: M.**

### 7. PubSub+ Cache

*`JCSMPSession.createCacheSession(…)`, supported by JCSMP per the matrix.*

Solves the late-joiner problem for publish-subscribe: a subscriber starting now sees only future
messages. A cache request returns the last value, or a window of history, for a topic — so a new
instance can initialise its state from the topic rather than from a database.

**Shape:** a `SolaceCacheTemplate` with `requestLatest(topic)` / `requestHistory(topic, count)`, and
optionally an `@SolaceListener(cacheOnStart = true)` that primes a listener before live delivery.
Requires a PubSub+ Cache deployment, so it must degrade cleanly when absent. **Effort: L.**

### 8. Topic dispatch

*Listed as supported for JCSMP.*

One flow or session, with per-topic listeners registered against it. Today each `@SolaceListener`
gets its own endpoint and flows; a service subscribing to twenty related topics pays for twenty
endpoints. Topic dispatch lets one endpoint fan out to different handler methods by topic.

**Shape:** several `@SolaceListener` methods sharing a `queue`, with the container dispatching by
matched subscription. A meaningful change to the container's dispatch path. **Effort: L.**

### 9. Structured Data Types as a payload format

*Supported; the library uses SDT only for headers.*

A `SdtSolaceMessageConverter` writing `SDTMap`/`SDTStream` bodies would let this library interoperate
with non-Java Solace applications that expect SDT rather than JSON — the usual case when talking to
existing C, .NET or JMS estates.

**Shape:** a second `SolaceMessageConverter`. Self-contained, no container changes. **Effort: S.**

### 10. `noLocal`

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

1. **Flow event handling** (#2) — small, and it unblocks both replay and a health indicator that can
   tell "connected" from "reconnecting".
2. **Flow tuning** (#1) — small, purely additive, and the transport window is the primary throughput
   knob for guaranteed messaging.
3. **Partitioned queues** (#4) — the largest conceptual gap against the Kafka model this library
   imitates.
4. **Queue browsing** (#3) — makes the dead message queue support usable in practice, which matters
   more now that `REJECTED` can put messages there deliberately.
5. **Distributed tracing** (#6) — the platform capability that most rewards the request-reply shape.

Items 1 and 2 are what remain of the "you cannot express that" answers in the consumer story; error
handling itself is now complete.

---

**Back to:** [README](../README.md)
