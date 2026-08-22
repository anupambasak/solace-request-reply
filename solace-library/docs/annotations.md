# `annotation` — the declarative API

Package `cris.prs.messaging.solace.annotation`.

---

## @EnableSolace

Enables detection of `@SolaceListener` methods, exactly as `@EnableKafka` does for `@KafkaListener`.
Imports `SolaceBootstrapConfiguration`, which registers the annotation post-processor and the
endpoint registry as infrastructure beans.

**Spring Boot applications do not normally need it** — the auto-configuration applies it when no
listener infrastructure is present. Declare it explicitly when auto-configuration is switched off, or
when building on plain Spring.

```java
@Configuration
@EnableSolace
public class MessagingConfiguration { }
```

---

## @SolaceListener

Marks a method as the target of a listener container — the direct counterpart of `@KafkaListener`.

All attributes are `String` so that they accept property placeholders:
`concurrency = "${app.concurrency:10}"`.

| Attribute | Default | Description |
| :--- | :--- | :--- |
| `id` | generated | Container id, used in logs and to look the container up in the registry. |
| `pattern` | none | `PUBLISH_SUBSCRIBE`, `POINT_TO_POINT` or `REQUEST_REPLY`. Sets the endpoint wiring that realises the pattern; see [Exchange patterns](exchange-patterns.md). |
| `topics` | `{}` | Topic subscriptions, e.g. `"orders/created"`, `"orders/>"`. |
| `queue` | `""` | Endpoint name. Ignored under `DIRECT`. Falls back to the container id. |
| `group` | `""` | Consumer group, appended as `<queue>.<group>`. |
| `endpointMode` | container default | `DURABLE_QUEUE`, `NON_DURABLE_QUEUE`, `DIRECT`. |
| `concurrency` | container default | Consumer flows. Each transactional flow takes one transacted session. |
| `selector` | `""` | Broker-side selector over the message's SDT user properties. |
| `transactional` | container default | Consume and reply inside a Solace local transaction. |
| `autoStartup` | container default | |
| `appendInstanceIdToQueue` | pattern default | Give this instance its own endpoint. |
| `appendInstanceIdToTopics` | `false` | Give this instance its own subscription. |
| `replyDestination` | `""` | Fixed reply destination; empty follows the request's `replyTo`. |
| `containerFactory` | `solaceListenerContainerFactory` | Bean name of the factory to build the container. |
| `dispatch` | container default | `INLINE` or `EXECUTOR`. `EXECUTOR` cannot be combined with `transactional = "true"`. |

Attributes set explicitly always win over the values a `pattern` would supply.

### Method signatures

| Parameter | Provided |
| :--- | :--- |
| the payload type | body converted by the `SolaceMessageConverter` |
| `Message<T>` | payload plus headers |
| `SolaceRecord<T>` | payload, destination, correlation id, `replyTo`, headers, raw message |
| `BytesXMLMessage` | the raw Solace message |
| `@Header`, `@Headers`, `@Payload` | standard Spring Messaging resolution |

A **non-void return value** is published to the destination in the request's `replyTo` (or to
`replyDestination`), with the correlation id copied — this is what makes request-reply work with no
output binding. Returning `void` makes the method a plain consumer. Returning a `Message<?>` lets you
set the destination and headers of the reply explicitly.

### Examples

```java
// Competing consumers over a shared durable endpoint, transactional.
@SolaceListener(pattern = "POINT_TO_POINT", topics = "work/submit",
        queue = "work", group = "workers",
        concurrency = "${app.workers:5}", transactional = "true")
public void onWork(WorkItem item) { process(item); }

// Fan-out: every instance receives every event.
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", topics = "events/created", queue = "events")
public void onEvent(Event event, @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String id) { }

// Request-reply: the return value goes back to the requester.
@SolaceListener(pattern = "REQUEST_REPLY", topics = {"orders/place", "orders/place/>"},
        queue = "orders", group = "svc", transactional = "true")
public Ack place(Order order) { return acknowledge(order); }

// Direct, non-persistent, lowest latency.
@SolaceListener(topics = "telemetry/>", endpointMode = "DIRECT")
public void onTelemetry(Reading reading) { }
```
