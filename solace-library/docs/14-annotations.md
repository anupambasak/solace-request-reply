# 14. Annotations

Two annotations. One you almost never write, one you write constantly.

---

## 14.1 `@EnableSolace`

```java
@Target(TYPE) @Retention(RUNTIME) @Documented
@Import(SolaceBootstrapConfiguration.class)
public @interface EnableSolace { }
```

Registers the two infrastructure beans that make `@SolaceListener` work:
`SolaceListenerAnnotationBeanPostProcessor` and `SolaceListenerEndpointRegistry`.

**In a Spring Boot application you do not write it.** `SolaceAutoConfiguration` imports
`SolaceAnnotationDrivenConfiguration`, which carries `@EnableSolace`, so annotation-driven listeners
are on by default — the same arrangement Spring Boot uses for `@EnableKafka`.

Write it when:

- the application is Spring but not Spring Boot, so nothing runs auto-configuration;
- a test assembles a context from explicit `@Configuration` classes.

It has no attributes, and registering it twice is harmless — the registrar checks for existing bean
definitions first.

---

## 14.2 `@SolaceListener`

```java
@Target({METHOD, ANNOTATION_TYPE}) @Retention(RUNTIME) @Documented
public @interface SolaceListener { … }
```

Declares that a method consumes from a Solace endpoint. Each annotated method becomes one
`SolaceListenerEndpoint`, which becomes one container.

`ANNOTATION_TYPE` is in the target list so you can build your own composed annotation:

```java
@Retention(RUNTIME) @Target(METHOD)
@SolaceListener(pattern = "POINT_TO_POINT", queue = "${app.queue}", group = "${app.group}")
public @interface OrderWorker { }
```

Discovery uses `AnnotatedElementUtils.findMergedAnnotation`, so meta-annotations and attribute
overrides resolve as they do anywhere else in Spring.

### Every attribute is a `String`

Including the numeric and boolean ones. That is the only way an annotation attribute can carry a
property placeholder, and it means the entire declaration is externally configurable:

```java
@SolaceListener(
        id           = "${app.orders.listener-id:orders}",
        pattern      = "POINT_TO_POINT",
        queue        = "${app.orders.queue}",
        group        = "${app.orders.group:workers}",
        topics       = {"${app.orders.topic}", "${app.orders.topic}/>"},
        concurrency  = "${app.orders.concurrency:5}",
        transactional= "${app.orders.transactional:true}",
        autoStartup  = "${app.orders.enabled:true}")
```

Placeholders are resolved through the `BeanFactory`'s embedded value resolver, so `${…}` and SpEL
`#{…}` both work, with the usual `:default` syntax.

---

## 14.3 Attribute reference

| Attribute | Default | Meaning |
| :--- | :--- | :--- |
| `id` | generated | Container id, used by the registry and in every log line. Generated as `solaceListenerEndpoint#<n>` when unset. Set it if you want to start/stop the container by hand. |
| `pattern` | none | `PUBLISH_SUBSCRIBE`, `POINT_TO_POINT` or `REQUEST_REPLY`. Fills in the endpoint wiring that realises the pattern — see [5. Exchange patterns](05-exchange-patterns.md). Empty leaves every default to the container factory. |
| `topics` | `{}` | Topic subscriptions attached to the endpoint. Solace wildcards apply: `*` one level, `>` one or more trailing levels. |
| `queue` | `""` | Endpoint name. Falls back to `id` when unset. Ignored for `endpointMode = DIRECT`. |
| `group` | `""` | Appended as `<queue>.<group>`. The consumer-group convention: same queue + same group = competing consumers. |
| `endpointMode` | from pattern / YAML | `DURABLE_QUEUE`, `NON_DURABLE_QUEUE` or `DIRECT`. |
| `concurrency` | from pattern / YAML | Number of flows bound to the endpoint. Clamped to 1 on a non-durable queue. |
| `selector` | `""` | Broker-side SQL92 predicate over message properties, e.g. `"region = 'EU' AND priority > 5"`. Filtering happens on the broker, so unmatched messages never cross the network. |
| `transactional` | from YAML (`false`) | Bind each flow to a `TransactedSession`. Forces `INLINE` dispatch; see [9. Transactions](09-transactions.md). |
| `errorOutcome` | from YAML | `ACCEPTED`, `FAILED`, `REJECTED` or `NONE` — what happens to a message whose listener throws. Ignored when `transactional` is set. See [7.6](07-consuming-messages.md#76-acknowledgement-settlement-and-errors). |
| `replayFrom` | `""` | `BEGINNING`, or an ISO-8601 instant. Re-delivers spooled messages on **every bind**, so it replays again on each restart — prefer the runtime operation. Affects the whole endpoint. See [7.11](07-consuming-messages.md#711-message-replay). |
| `topicDispatch` | `""` | `"true"` shares one endpoint with the other listeners declaring the same `queue` and `group`, routing by matched subscription. Every member must declare it. See [7.10](07-consuming-messages.md#710-topic-dispatch--several-methods-one-endpoint). |
| `dispatch` | from YAML (`INLINE`) | `INLINE` or `EXECUTOR`. `EXECUTOR` with `transactional=true` fails at startup. |
| `autoStartup` | from YAML (`true`) | Start with the context, or wait for `registry.getListenerContainer(id).start()`. |
| `appendInstanceIdToQueue` | from pattern | Append the instance id to the endpoint name, making it private to this instance. This one attribute is the difference between fan-out and competing consumers. |
| `appendInstanceIdToTopics` | `false` | Append the instance id as an extra topic *level* on every subscription. For addressing one specific instance. |
| `replyDestination` | `""` | Where a returned value is published, overriding the request's `replyTo`. Leave empty for normal request-reply — the requester owns the reply channel. |
| `containerFactory` | `solaceListenerContainerFactory` | Bean name of the `SolaceListenerContainerFactory` to build this container. |

### Validation

At registration the post-processor asserts that the endpoint declares **topics or a queue**. A
listener with neither has nothing to bind to and fails fast with the offending method in the message.

---

## 14.4 Method signatures

The method may take any combination of:

| Parameter | Gives you |
| :--- | :--- |
| the payload type — `Order order` | the converted body |
| `Message<Order>` | payload plus headers, Spring-style |
| `SolaceRecord<Order>` | payload plus destination, correlation id, replyTo, headers, redelivered flag, raw message |
| `BytesXMLMessage` | the raw JCSMP message |
| `@Header("solace_correlationId") String id` | one header |
| `@Header(name = "tenant", required = false) String tenant` | one optional user property |
| `@Headers Map<String,Object> headers` | all of them |
| `MessageHeaders headers` | all of them, typed |

The **payload type** is derived from the first parameter that is not framework machinery —
`@Header`/`@Headers`/`MessageHeaders`/`BytesXMLMessage` are skipped, and `Message<T>` /
`SolaceRecord<T>` are unwrapped to `T`. That single type is what the converter is asked to produce.
A listener that takes only headers and the raw message performs no body conversion at all.

### The return value is the reply

```java
@SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", topics = "pricing/quote")
public Quote quote(PriceRequest request) {
    return new Quote(request.sku(), price(request));   // published to the request's replyTo
}
```

- `void` or `null` → nothing is published; a one-way listener.
- a value → published to the destination resolved as: a `solace_targetDestination` header on a
  returned `Message`, then `replyDestination`, then the request's `replyTo`.
- a value with nowhere to go → logged and dropped, **not** thrown. The message itself was handled
  successfully; failing it would cause a pointless redelivery.

Return a `Message<?>` to control the reply's headers:

```java
public Message<Quote> quote(PriceRequest request) {
    return MessageBuilder.withPayload(new Quote(...))
            .setHeader("pricingVersion", "v2")
            .build();
}
```

---

## 14.5 Worked declarations

**Broadcast to every instance**

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "config", topics = "config/changed")
public void onConfigChange(ConfigChange change) { … }
```
→ temporary queue `config.<instance-id>`, exclusive, one flow. Every pod gets a copy.

**Shared work queue**

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "tasks", group = "workers",
        topics = "tasks/submit", concurrency = "10")
public void onTask(Task task) { … }
```
→ durable queue `tasks.workers`, non-exclusive, ten flows per pod. Each task goes to exactly one.

**Responder, transactional**

```java
@SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", group = "v1",
        topics = {"pricing/quote", "pricing/quote/>"}, concurrency = "5", transactional = "true")
public Quote quote(PriceRequest request) { … }
```
→ durable queue `pricing.v1`, non-exclusive, five transacted flows. Reply and acknowledgement commit
together.

**Rejecting a message that will never succeed**

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers",
        topics = "orders/created", errorOutcome = "REJECTED")
public void onOrder(Order order) { … }
```
→ a failing message goes straight to the dead message queue instead of being retried
`max-redelivery-count` times first. The container negotiates the outcome on the flow automatically.

**Filtered subscription**

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "eu",
        topics = "orders/>", selector = "region = 'EU'")
public void onEuOrder(Order order) { … }
```
→ the broker evaluates the selector; non-EU orders never reach this consumer.

**Addressed to one instance**

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "control", topics = "control/drain",
        appendInstanceIdToTopics = "true")
public void onDrain() { … }
```
→ subscribes to `control/drain/<instance-id>`. Publishing to that exact topic reaches one pod.

**Several methods sharing one endpoint**

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "notifications", group = "v1",
        topics = "routed/incident/*", topicDispatch = "true")
public void onIncident(Notification notification) { … }

@SolaceListener(pattern = "POINT_TO_POINT", queue = "notifications", group = "v1",
        topics = "routed/>", topicDispatch = "true")     // declared last: first match wins
public void onAnythingElse(SolaceRecord<Notification> record) { … }
```
→ one durable queue `notifications.v1` with both subscriptions, routed by topic. Each method keeps its
own payload type.

**Rebuilding from history**

```java
@SolaceListener(id = "rebuild", pattern = "POINT_TO_POINT", queue = "orders", group = "rebuild",
        topics = "orders/>", replayFrom = "BEGINNING", autoStartup = "false")
public void rebuild(Order order) { … }
```
→ registered but idle; start it when a rebuild is wanted. Note `replayFrom` replays on **every** bind.

**Started manually**

```java
@SolaceListener(id = "backfill", pattern = "POINT_TO_POINT", queue = "backfill",
        topics = "backfill/run", autoStartup = "false")
public void onBackfill(BackfillJob job) { … }
```
```java
registry.getListenerContainer("backfill").start();
```

---

**Previous:** [13. Spring integration](13-spring-integration.md)  ·  [Index](00-index.md)  ·  **Next:** [15. Configuration](15-configuration.md)
