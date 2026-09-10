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
| Flow event handling and active flow indication | `SolaceFlowListener`, `SolaceFlowEvent`, container `isActive()` / `isDegraded()` |
| Consumer flow tuning | `ContainerProperties.Flow`, `solace.listener.flow.*` |
| Session event handling | `SolaceSessionListener`, `SolaceSessionEvent`, `SolaceSessionState` |
| Broker-side session statistics | `SolaceSessionStatistics`, `solace.metrics.session-statistics` |
| `noLocal` | `solace.listener.flow.no-local` |
| Queue browsing | `SolaceOperations.browse(...)`, `SolaceBrowser`, `BrowseSpec` |
| Message replay | `ReplayStartPoint`, `@SolaceListener(replayFrom = ...)`, `container.replay(...)` |
| Topic dispatch | `@SolaceListener(topicDispatch = "true")`, `TopicDispatchingSolaceListener` |
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

The Micrometer implementations live in a new `org.cris.prs.messaging.solace.observability` package and are
registered by `SolaceObservabilityConfiguration` when a `MeterRegistry` bean is present. Counters and
timers are recorded as messages flow; the state gauges are registered by `SolaceMetricsBinder`, a
`SmartLifecycle` in the highest phase — a Micrometer `MeterBinder` would have bound before the
listener containers were registered, which happens in the annotation post-processor's
`afterSingletonsInstantiated`.

Meters: `solace.listener.messages.received`, `solace.listener.processing`, `solace.listener.running`,
`solace.listener.flows`, `solace.requests.sent`, `solace.requests.send.failed`,
`solace.requests.latency`, `solace.requests.timeouts`, `solace.requests.pending`,
`solace.replies.unmatched`. See [16. Operations](16-operations.md#162-micrometer-metrics).

**Resolved since:** broker-side statistics are now sampled — a curated set of JCSMP `StatType`
counters published as `solace.session.*`, replaceable through `solace.metrics.session-statistics`.
Each gets its own meter name rather than one meter tagged by name, because they do not share a unit.

**Still open:** per-endpoint spool depth, which is only available through SEMP — a management API,
not the client one, so it is out of this library's scope. Monitor it from the broker.

### Actuator health indicator

`SolaceHealthIndicator` contributes `/actuator/health/solace` from state already held in memory — it
never contacts the broker, so it is cheap enough for a readiness probe. It reports DOWN when the
session factory says its connection is gone, or when a registered container is not running.

That second rule is configurable (`solace.health.require-all-containers-running`) because a container
declared with `autoStartup = "false"`, or stopped deliberately through the registry, is not a fault —
and reporting it as one would keep the instance out of the load balancer indefinitely.

`SolaceSessionFactory` gained a `default boolean isHealthy()` for this. A default method rather than a
new abstract one, so a custom session factory keeps compiling and is simply reported as healthy.

**Resolved since:** the indicator now distinguishes *connected* from *reconnecting*, using the flow
events described below — a container reports `degraded [RECONNECTING]` and the health status goes
DOWN. What remains unmodelled is a **session**-level reconnect that leaves flows untouched; that would
need `SessionEventHandler`, which nothing here subscribes to.

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

**Resolved since:** success now goes through `settle(ACCEPTED)` too, so one method decides every
message's fate and there is no second way to acknowledge that a future outcome would have to be
threaded through. This is cosmetic — the two are identical on the wire, and `ACCEPTED` is the one
outcome that never needs negotiating at bind time. Because that equivalence is a property of the
broker rather than of this library, a failed settle falls back to `ackMessage()` for the rest of the
container's life and logs once, so a tidying change cannot become an outage.

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

### Flow event handling

*Was Tier 1, item 2. `FlowEventHandler`, `FlowEventArgs`, `FlowEvent`,
`ConsumerFlowProperties.setActiveFlowIndication(boolean)`, and the four-argument
`createFlow(listener, flowProps, endpointProps, flowEventHandler)` — which exists on both
`JCSMPSession` and `TransactedSession`.*

`SolaceFlowEvent` maps JCSMP's six events; `SolaceFlowListener` receives a `SolaceFlowEventArgs`
carrying container id, flow index, endpoint, event, info, exception and response code. A value object
rather than a widened callback signature, so a future JCSMP field does not break every implementation.

Logging is unconditional and level-graded by what an operator needs: `DOWN` is an **error** because
the flow will not recover without a container restart, `RECONNECTING` is a **warning** because
consumption has stopped for now, the rest are informational. A listener adds to that rather than
replacing it.

The backlog predicted two features from one change, and both landed:

- **Operational visibility.** `isDegraded()` is the difference between "running" and "actually
  consuming" — a container stays running throughout a reconnect. The Actuator health indicator and a
  `solace.listener.degraded` gauge both use it, which is what closed the health-indicator gap above.
- **Leader election.** Active flow indication is requested automatically for `EXCLUSIVE` endpoints,
  so `ACTIVE`/`INACTIVE` events arrive without configuration and `isActive()` answers "am I the
  leader" with no extra coordination.

One judgement worth recording: **`INACTIVE` is deliberately not degraded.** A standby flow on an
exclusive endpoint is healthy and working as designed; counting it as degraded would fail the health
check of every instance that is not the leader — which is most of them.

**Resolved since:** see *Session event handling* below. The health indicator now reports four session
states rather than two, and `reconnecting` is distinct from `down`.

### Consumer flow tuning

*Was Tier 1, item 1. `setTransportWindowSize`, `setAckThreshold`, `setAckTimerInMsecs`,
`setWindowedAckMaxSize`, `setReconnectTries`, `setReconnectRetryIntervalInMsecs`.*

`ContainerProperties.Flow`, bound from `solace.listener.flow.*`, mirroring the existing `Endpoint`
block as the backlog proposed. Every field is a **nullable** boxed type and `applyTo` writes only what
was set, so an untouched block is a genuine no-op — which is the property a purely additive change has
to have, and the one the test pins down.

`ackTimer` and `reconnectRetryInterval` are `Duration` rather than raw millis, converted at the
boundary, because that is what a Spring Boot user expects to write (`1s`, `250ms`).

There is deliberately no `@SolaceListener` attribute for any of it: there are seven properties, they
are rarely per-listener, and a second container factory already expresses per-listener tuning.

Worth being clear about the distinction from the endpoint block: endpoint properties are applied only
when a queue is **first provisioned** and thereafter ignored by the broker, while flow properties are
applied on **every bind** — so a change here takes effect on the next restart with no need to touch
the queue.

**Resolved since:** `noLocal` was added to the same block as its own documented feature — see below.

### Session event handling

*`SessionEventHandler`, `SessionEventArgs`, `SessionEvent`, and
`SpringJCSMPFactory.createSession(Context, SessionEventHandler)`.*

The layer below flow events. JCSMP reconnects a session transparently, so a network blip that stops
**all** traffic for seconds leaves no trace: flows that survive raise no flow event, no message is
lost, and nothing notices. `SolaceSessionEvent` maps JCSMP's seven events and `SolaceSessionListener`
receives a `SolaceSessionEventArgs`.

Verifying the API first paid off here: `SpringJCSMPFactory.createSession()` decompiles to exactly
`createSession(null, null)`, so passing a handler is a strictly additive change to the existing call
rather than a different code path.

`SolaceSessionState` is four states rather than a boolean, and that is the substance of the feature.
`RECONNECTING` is neither healthy nor permanently broken; collapsing it into `DOWN` would restart-loop
a pod through a network blip, and collapsing it into `CONNECTED` would keep the pod taking traffic it
cannot serve. `NOT_CONNECTED` is healthy on the same reasoning — an application that has not yet
needed the broker is not broken. `getSessionState()` and `getSessionStatistics()` are both `default`
methods so a custom `SolaceSessionFactory` keeps compiling.

The event most worth handling is `VIRTUAL_ROUTER_NAME_CHANGED`: the session came back on the *other*
broker of an HA pair, and temporary endpoints — every pub/sub queue and reply destination in the
application — plus unreplicated unacknowledged messages did not come with it.

**Still open:** nothing on this item. Session and flow events together cover both layers.

### Broker-side session statistics

*`Session.getSessionStats()`, `JCSMPStats.getStat(StatType)`, `StatType.fromString(String)`.*

JCSMP keeps around seventy counters on a session; publishing all of them would bury the useful ones
and multiply the time series for nothing. `SolaceSessionStatistics.DEFAULTS` is a curated sixteen,
grouped by the question each answers: throughput, trouble (retransmits, discards, rejections, ack
timeouts), back-pressure (window closures), and connection churn. `solace.metrics.session-statistics`
replaces the list with any `StatType` names; an unrecognised one is logged and skipped rather than
failing startup, and an empty list turns the feature off.

Each statistic gets its **own meter name** — `TOTAL_MSGS_SENT` becomes
`solace.session.total.msgs.sent` — rather than one meter tagged by name. They do not share a unit, and
mixing message counts with byte counts under one name makes every aggregate meaningless.

They are counters JCSMP owns, so they are registered as `FunctionCounter`s reading through to the
session. Sampling never opens a connection: a factory with no session yet reports zero.

The pair worth knowing is `publisher.window.closed` and `subscriber.flow.window.closed` — look at
those *before* touching `solace.listener.flow.transport-window-size`, because a window that never
closes does not need enlarging.

### `noLocal`

*Was Tier 2, item 8. `ConsumerFlowProperties.setNoLocal(boolean)`.*

A one-line addition to `ContainerProperties.Flow`, deliberately deferred from the flow-tuning change
so it arrived with its own documentation rather than smuggled in. It earns that documentation, because
two things about it surprise people:

- **Solace matches on the client connection, not the application.** The library shares one session for
  publishing and non-transactional consuming, so it works — but a **transactional** container opens
  its own connection, so its publishes are already elsewhere and `noLocal` does nothing there.
- **It is a per-flow filter, not a discard.** On a shared durable queue the message is not delivered
  to *this* instance but is still delivered to another. It suppresses local delivery; it does not
  remove the message.

### Queue browsing

*`JCSMPSession.createBrowser(BrowserProperties)`, `Browser.getNextNoWait()`, `Browser.remove(...)`.*

`SolaceOperations.browse(queue, type)` returns an `AutoCloseable` `SolaceBrowser` over what is spooled
on an endpoint, read **without acknowledging**. The backlog asked for a stream of `SolaceRecord`; it
also got `next()`, `take(int)` and `remove(record)`, because a browse is used interactively as often
as it is used in a pipeline.

Two implementation details worth recording. The stream is genuinely lazy, so `findFirst()` or
`.limit(n)` stops the browse rather than draining the queue first. And a zero wait timeout uses
`getNextNoWait()` rather than `getNext(0)` — **JCSMP reads a zero timeout as "wait forever"**, which
would hang at the end of every queue.

`remove` is the one destructive operation and is documented as such: it is what makes browsing a DMQ
useful (inspect a poison message, then drop it) and the reason not to point a browse at a live work
queue by accident.

**Still open:** queue depth as a number. Counting by walking is what the client API allows; a real
depth is a SEMP question, and SEMP is out of scope.

### Message replay

*JCSMP 10.11+. `ConsumerFlowProperties.setReplayStartLocation(...)`,
`JCSMPFactory.createReplayStartLocationBeginning()` / `createReplayStartLocationDate(Date)`.*

Both shapes the backlog proposed, because they answer different questions.
`@SolaceListener(replayFrom = "BEGINNING" | ISO-8601)` is the deployment-time form — occasionally what
you want for a rebuild-on-boot projection, and documented as replaying on *every* restart, which
usually is not. `DefaultSolaceMessageListenerContainer.replay(ReplayStartPoint)` is the operational
form, and the backlog was right that it is the more useful one.

`ReplayStartPoint` is immutable and compared by value, parses `BEGINNING` or an ISO-8601 instant, and
fails at startup on anything else naming what was expected.

The three consequences are documented prominently because none is obvious: replay affects the **whole
endpoint**, so every consumer of a shared queue receives the replayed messages; handlers see messages
again and `isRedelivered()` does **not** mark a replayed message; and a replay the broker cannot
satisfy fails the flow, arriving as a `DOWN` flow event rather than as an error from the call. That
last one is exactly why flow event handling was the prerequisite — without it, a failed replay would
leave a silently non-consuming container.

### Topic dispatch

*Client-side, by design.*

Several `@SolaceListener` methods sharing a `queue` and `group`, with the container binding **one**
endpoint carrying the union of their subscriptions and routing each message to the method whose
subscription matched. Each method keeps its own payload type, which is the point.

**JCSMP's native topic dispatch was rejected after checking the jar**: it needs a
`ConsumerNotificationDispatcherFactory` from `com.solacesystems.jcsmp.protocol.nio.impl`, an internal
package. `SolaceTopicMatcher` implements the broker's wildcard rules client-side instead — `*` is
exactly one level, `>` is one or more trailing levels and only as the final character — which is both
portable and testable without a broker.

Three deliberate decisions:

- **Opt-in via `topicDispatch = "true"`, not implicit by shared queue name.** Merging listeners that
  merely happen to share a queue would silently change what an existing application does.
- **First match wins, in declaration order, not by specificity.** Simple and predictable; the docs say
  to declare specific subscriptions before catch-alls, and the demo does.
- **Members must agree on `pattern`, `endpointMode`, `concurrency`, `transactional`, `selector` and
  `accessType`.** They share one flow set, so honouring a later member's setting would mean ignoring
  the first — a startup failure naming both methods beats a silent drop.

An unmatched message is acknowledged rather than failed: nothing about a retry makes a subscription
match that did not match the first time, so failing it would fill the endpoint with messages no method
wants. The warning it logs is the useful signal, and usually means a durable endpoint still carries a
subscription from an earlier version of the code.

**Still open:** the backlog rated this **L** on the assumption it changed the container's dispatch
path. It did not — the merge happens in the annotation post-processor and the container is unchanged,
which is why it came in nearer **M**.

---

## Tier 2 — new capability, larger surface

### 1. Partitioned queues

*JCSMP 10.19+.*

`POINT_TO_POINT` today gives competing consumers with **no ordering guarantee** — two messages about
the same entity can be processed concurrently by different workers. Partitioned queues fix that: the
broker hashes a partition key and pins every message with the same key to one consumer, giving
Kafka-style per-key ordering while still scaling out.

For a library modelled on Spring for Apache Kafka this is arguably the biggest conceptual gap: Kafka
users expect per-key ordering, and today's point-to-point silently does not provide it.

**Shape:** a `partitionKey` on send (a message property), a partition count on endpoint provisioning,
and documentation of the ordering guarantee in `exchange-patterns.md`. **Effort: M.**

### 2. Distributed tracing

*JCSMP 10.17+ manual, 10.26+ auto-instrumentation; `solace-opentelemetry-jcsmp-integration`.*

Called out on the platform page: tracking a message from the sending application, between brokers, to
the receiving one. The library has exactly the shape that benefits — a request-reply hop where the
correlation id already exists but carries no trace context, so a request and its reply appear as two
unrelated spans.

**Shape:** propagate the OpenTelemetry context into message properties on send and extract it into
the listener's scope, behind an optional dependency so tracing stays opt-in. **Effort: M.**

### 3. PubSub+ Cache

*`JCSMPSession.createCacheSession(…)`, supported by JCSMP per the matrix.*

Solves the late-joiner problem for publish-subscribe: a subscriber starting now sees only future
messages. A cache request returns the last value, or a window of history, for a topic — so a new
instance can initialise its state from the topic rather than from a database.

**Shape:** a `SolaceCacheTemplate` with `requestLatest(topic)` / `requestHistory(topic, count)`, and
optionally an `@SolaceListener(cacheOnStart = true)` that primes a listener before live delivery.
Requires a PubSub+ Cache deployment, so it must degrade cleanly when absent. **Effort: L.**

### 4. Structured Data Types as a payload format

*Supported; the library uses SDT only for headers.*

A `SdtSolaceMessageConverter` writing `SDTMap`/`SDTStream` bodies would let this library interoperate
with non-Java Solace applications that expect SDT rather than JSON — the usual case when talking to
existing C, .NET or JMS estates.

**Shape:** a second `SolaceMessageConverter`. Self-contained, no container changes. **Effort: S.**

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

Tier 1 is empty. What remains is genuinely larger work, in rough order of value:

1. **Distributed tracing** (#2) — the platform capability that most rewards the request-reply shape,
   and the header mapper is already the natural place to carry context. The smallest of what is left.
2. **Partitioned queues** (#1) — the largest conceptual gap against the Kafka model this library
   imitates, and the one a Kafka user asks about first.
3. **Batch listeners** — not a Solace feature but the most-missed piece of the Spring for Kafka
   analogy, and the only remaining item that changes the container's dispatch path.
4. **PubSub+ Cache** (#3) — worthwhile, but a distinct client-side subsystem rather than an addition
   to what exists.

---

**Back to:** [README](../README.md)
