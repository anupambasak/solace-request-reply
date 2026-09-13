# 18. Feature catalogue

Everything the library implements today, grouped by area, with what each feature does, where it is
configured or invoked, and which guide explains it. This is the "what's in the box" companion to the
[19. Feature backlog](19-feature-backlog.md), which covers what is *not* yet built.

---

## 18.1 Programming model

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Template-based sending | Six `send` overloads, delivery defaults, header mapping, transaction-aware producer | `SolaceTemplate` / `SolaceOperations` | [6](06-producing-messages.md) |
| Annotation-driven listeners | A method becomes a running consumer; flexible argument resolution; the return value is the reply | `@SolaceListener` | [7](07-consuming-messages.md), [14](14-annotations.md) |
| Request-reply with correlated futures | `sendAndReceive` returning a `RequestReplyFuture` (a `CompletableFuture` + latency) | `ReplyingSolaceTemplate` | [8](08-request-reply.md) |
| Auto-configuration | Put the library on the classpath and every bean is wired; `@EnableSolace` applied for you | `SolaceAutoConfiguration` | [13](13-spring-integration.md) |
| Reactive bridge | `Mono.fromFuture(future)` — no blocking imposed on the reactive path | `RequestReplyFuture extends CompletableFuture` | [8.2](08-request-reply.md#82-the-requester) |

## 18.2 Exchange patterns

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Publish-subscribe | Fan-out: one temporary endpoint per instance, every instance gets a copy | `@SolaceListener(pattern = "PUBLISH_SUBSCRIBE")` | [5.3](05-exchange-patterns.md#53-publish_subscribe--every-instance-gets-a-copy) |
| Point-to-point | Competing consumers on one shared durable queue | `@SolaceListener(pattern = "POINT_TO_POINT")` | [5.4](05-exchange-patterns.md#54-point_to_point--exactly-one-consumer) |
| Request-reply | Return a value; it is published to the requester's `replyTo` | `@SolaceListener(pattern = "REQUEST_REPLY")` | [5.5](05-exchange-patterns.md#55-request_reply--a-response-comes-back) |
| Pattern defaults | One word fills in endpoint mode, access type, instance-id append and concurrency; explicit attributes always win | `ExchangePattern`, `applyPatternDefaults()` | [5.2](05-exchange-patterns.md#52-what-each-pattern-sets) |
| Consumer groups | `<queue>.<group>` naming gives Kafka-style groups without a broker protocol | `queue` + `group` | [5.4](05-exchange-patterns.md#54-point_to_point--exactly-one-consumer) |
| Broker-side selectors | SQL92 predicate filters before the network | `@SolaceListener(selector = …)` | [14.3](14-annotations.md#143-attribute-reference) |
| Topic dispatch | Several methods share one endpoint, routed by matched subscription, each keeping its own payload type | `@SolaceListener(topicDispatch = "true")` | [7.10](07-consuming-messages.md#710-topic-dispatch--several-methods-one-endpoint) |

## 18.3 Endpoints, delivery and reliability

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Three endpoint modes | Durable queue, non-durable (temporary) queue, direct (no endpoint) | `EndpointMode`, `solace.listener.endpoint-mode` | [15.5](15-configuration.md#155-endpointmode-in-detail) |
| Endpoint provisioning | Creates durable queues and the DMQ, attaches subscriptions, tolerates the "already exists" cases | `provision-endpoint`, `ContainerProperties.Endpoint` | [7.3](07-consuming-messages.md#73-startup) |
| Concurrency | N independent flows per container | `concurrency` | [7.4](07-consuming-messages.md#74-concurrency) |
| Settlement outcomes | `ACCEPTED` / `FAILED` / `REJECTED` / `NONE` per container or per failure | `SettlementOutcome`, `error-outcome`, `resolveOutcome` | [7.6](07-consuming-messages.md#76-acknowledgement-settlement-and-errors) |
| Delivery count | How many times a message was delivered, with a capability guard | `SolaceRecord.getDeliveryCount()`, `SolaceHeaders.DELIVERY_COUNT` | [7.7](07-consuming-messages.md#77-delivery-count) |
| Redelivery + DMQ | `max-redelivery-count` and the dead message queue for poison messages | `ContainerProperties.Endpoint.DeadMessageQueue` | [7.12](07-consuming-messages.md#712-redelivery-and-the-dead-message-queue) |
| Message replay | Re-deliver spooled messages from a start point, as config or as an operation | `ReplayStartPoint`, `container.replay(...)`, `replayFrom` | [7.11](07-consuming-messages.md#711-message-replay) |
| Queue browsing | Read a queue **without consuming it**, including the DMQ; destructive `remove` | `SolaceOperations.browse(...)`, `SolaceBrowser`, `BrowseSpec` | [6.9](06-producing-messages.md#69-browsing-a-queue) |

## 18.4 Threading, dispatch and lifecycle

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Inline / executor dispatch | Run the listener on the delivery thread, or hand it to a bounded task executor with back-pressure | `dispatch`, `dispatch-queue-capacity` | [7.5](07-consuming-messages.md#75-dispatch-modes) |
| Keep-alive thread | A non-daemon thread keeps a listener-only app alive | `solace.listener.keep-alive`, `ContainerKeepAlive` | [7.15](07-consuming-messages.md#715-the-keep-alive-thread) |
| Phased start/stop | Containers consume before the reply template sends; requester stops first on shutdown | `SmartLifecycle` phases | [13.7](13-spring-integration.md#137-lifecycle-and-phases) |
| Manual container control | Start/stop containers by id; register listeners programmatically | `SolaceListenerEndpointRegistry`, `autoStartup` | [7.13](07-consuming-messages.md#713-lifecycle-and-manual-control) |

## 18.5 Transactions

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Solace local transactions | `@Transactional` / `TransactionTemplate` over a JCSMP `TransactedSession` | `SolaceTransactionManager` | [9](09-transactions.md) |
| Transacted listeners | Consume-and-reply commit atomically | `@SolaceListener(transactional = "true")` | [9.4](09-transactions.md#94-consumer-side-transactions) |
| Transacted-session budget | Each transactional container gets its own connection; startup assertion against the per-connection cap | `max-transacted-sessions-per-connection` | [9.6](09-transactions.md#96-the-transacted-session-budget) |
| Transaction-aware sending | The same `send` publishes immediately or enlists, with no API difference | `isTransactionActive()` | [9.3](09-transactions.md#93-producer-side-transactions) |

## 18.6 Conversion, headers and multi-instance

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Pluggable body conversion | Jackson by default over a two-method SPI | `SolaceMessageConverter`, `JacksonSolaceMessageConverter` | [10.1](10-conversion-and-headers.md#101-solacemessageconverter) |
| Header mapping | Spring `MessageHeaders` ↔ Solace fields + SDT user properties | `SolaceHeaderMapper`, `SolaceHeaders` | [10.2](10-conversion-and-headers.md#102-solaceheadermapper) |
| Rich consumer record | Payload plus destination, correlation id, redelivered flag, delivery count, raw message | `SolaceRecord<T>` | [10.3](10-conversion-and-headers.md#103-solacerecordt) |
| Per-instance destinations | Reply topics and fan-out queues carry a sanitised pod/host id | `InstanceIdProvider`, `HostnameInstanceIdProvider` | [11](11-multi-instance.md) |
| Split reply destinations | Give a service its own reply endpoint when the shared one is not enough | `ReplyEndpointSpec`, `ReplyingSolaceTemplateFactory` | [8.6](08-request-reply.md#86-when-to-split-a-reply-destination) |

## 18.7 Schema registry (optional — Apicurio)

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Avro / Protobuf / JSON Schema | Governed, versioned payloads through the same code as plain JSON | `SchemaRegistrySolaceMessageConverter`, `solace.schema-registry.*` | [12](12-schema-registry.md) |
| Topic-profile artifact resolution | Solace wildcard expressions → schema artifacts, safe for per-instance reply topics | `SolaceTopicProfileStrategy`, `artifact-resolver-strategy` | [12.5](12-schema-registry.md#125-where-the-schema-comes-from-artifact-resolution) |
| Startup / first-message registration | Publish declared and derived schemas, with fail-fast control | `SchemaArtifactRegistrar`, `registration.*` | [12.5](12-schema-registry.md#125-where-the-schema-comes-from-artifact-resolution) |
| Fault-tolerant caching | Keep serving cached schemas through a registry outage | `cache.fault-tolerant-refresh` (default on) | [12.6](12-schema-registry.md#126-registry-availability-and-caching) |
| Schema-aware error handling | Reject non-retryable schema failures straight to the DMQ | `SchemaRegistryErrorHandler` | [12.7](12-schema-registry.md#127-failures-and-settlement) |
| Pluggable codec seam | Another registry or a test fake behind the byte-level SPI | `SchemaCodec`, `SchemaCodecs.of(...)` | [16.1](16-extension-points.md#161-the-map) |

## 18.8 Observability (optional — Micrometer / Actuator)

| Feature | What it gives you | Entry point | Guide |
| :--- | :--- | :--- | :--- |
| Listener meters | Throughput, processing time, settlement, flow events, running/degraded/active gauges | `SolaceListenerMetrics`, `MicrometerSolaceListenerMetrics` | [20.2](20-operations.md#202-micrometer-metrics) |
| Request-reply meters | Requests sent/failed, latency, timeouts, pending, unmatched replies | `SolaceRequestReplyMetrics` | [20.2](20-operations.md#202-micrometer-metrics) |
| Broker-side session statistics | A curated set of JCSMP `StatType` counters, replaceable | `SolaceSessionStatistics`, `session-statistics` | [20.2](20-operations.md#202-micrometer-metrics) |
| Actuator health indicator | `/actuator/health/solace`, distinguishing running from actually-consuming | `SolaceHealthIndicator` | [20.3](20-operations.md#203-actuator-health) |
| Flow event handling | React to reconnects and active-consumer changes; leader election on exclusive endpoints | `SolaceFlowListener`, `SolaceFlowEvent`, `isActive()`/`isDegraded()` | [7.8](07-consuming-messages.md#78-flow-events) |
| Session event handling | See transparent JCSMP reconnects and HA failovers | `SolaceSessionListener`, `SolaceSessionEvent`, `SolaceSessionState` | [11.7](11-multi-instance.md#117-session-events) |

## 18.9 Extensibility

Every collaborator is an interface with a default registered `@ConditionalOnMissingBean` — replace one
by declaring a bean. The complete map is in [16. Extension points](16-extension-points.md); the summary:

| Replace | Default | To |
| :--- | :--- | :--- |
| `SolaceMessageConverter` | Jackson (or registry converter) | change the wire format |
| `SolaceHeaderMapper` | `DefaultSolaceHeaderMapper` | add tracing, filter headers |
| `SolaceSessionFactory` | `DefaultSolaceSessionFactory` | pooling, per-tenant connections |
| `InstanceIdProvider` | `HostnameInstanceIdProvider` | change instance naming |
| `SolaceListenerErrorHandler` | logging | dead-letter routing, per-failure outcomes |
| `SolaceFlowListener` / `SolaceSessionListener` | logging only | act on reconnects / HA failover |
| `AsyncTaskExecutor` (`solaceListenerTaskExecutor`) | `SimpleAsyncTaskExecutor` | bound the pool, propagate MDC |
| `SolaceListenerMetrics` / `SolaceRequestReplyMetrics` | Micrometer or `NO_OP` | report elsewhere |
| `SchemaCodecs` | Apicurio codecs | another registry, a test fake |
| a second `SolaceListenerContainerFactory` | the default | per-listener tuning / error handling |
| a second `ReplyingSolaceTemplate` | the auto-configured one | a dedicated reply destination |

---

**Previous:** [17. Class reference](17-class-reference.md)  ·  [Index](00-index.md)  ·  **Next:** [19. Feature backlog](19-feature-backlog.md)
