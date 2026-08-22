# `listener` — containers, endpoints, annotation processing

Package `cris.prs.messaging.solace.listener`.

---

## SolaceMessageListener

Functional interface, the low-level listener contract: `void onMessage(BytesXMLMessage) throws
Exception`. The container acknowledges (or commits) after a normal return. Analogue of Kafka's
`MessageListener`.

## SolaceListenerErrorHandler

Functional interface: `void handleError(BytesXMLMessage, Exception)`. The default implementation logs
the failure with the destination. Whether the message is then acknowledged is decided by
`ackOnError` (non-transactional) or by rollback (transactional).

## SolaceMessageListenerContainer

A running listener, extending `SmartLifecycle`.

| Method | Description |
| :--- | :--- |
| `getListenerId()` | The container's id, used in logs and in the registry. |
| `setupMessageListener(SolaceMessageListener)` | Set the listener before start. |

---

## SolaceListenerEndpoint

Everything the container factory needs to build a container — the analogue of
`KafkaListenerEndpoint`. A mutable value object; `null` on a field means "use the container factory
default".

| Field | Type | Meaning when null / false |
| :--- | :--- | :--- |
| `id` | `String` | generated as `solaceListenerEndpoint#N` |
| `topics` | `List<String>` | no subscriptions |
| `queue` | `String` | falls back to the id as the endpoint name |
| `group` | `String` | no group segment |
| `pattern` | `ExchangePattern` | no preset applied |
| `endpointMode` | `EndpointMode` | container default |
| `accessType` | `ContainerProperties.AccessType` | container default |
| `selector` | `String` | no broker-side selector |
| `concurrency` | `Integer` | container default |
| `transactional` | `Boolean` | container default |
| `autoStartup` | `Boolean` | container default |
| `appendInstanceIdToQueue` | `Boolean` | not appended |
| `appendInstanceIdToTopics` | `Boolean` | not appended |
| `replyDestination` | `String` | replies follow the request's `replyTo` |
| `payloadType` | `Class<?>` | `Object` — the raw message is passed through |
| `invocableHandlerMethod` | `InvocableHandlerMethod` | set for `@SolaceListener` methods |
| `messageListener` | `SolaceMessageListener` | set for programmatic registration |

| Method | Description |
| :--- | :--- |
| `applyPatternDefaults()` | Fill in the wiring implied by `pattern`, leaving anything already set alone. Call after every explicit attribute has been applied. |
| `resolveQueueName(String instanceId)` | `<queue>[.<group>][.<instanceId>]`. The instance id is appended only when `appendInstanceIdToQueue` is true — that flag is the whole difference between fan-out and competing consumers. |
| `resolveTopics(String instanceId)` | The subscriptions, with the instance id appended as an extra level when `appendInstanceIdToTopics` is true. |

---

## ContainerProperties

Tuning shared by every container from one factory; see the
[configuration reference](configuration.md#listener-defaults--solacelistener) for defaults.

Nested types:

| Type | Purpose |
| :--- | :--- |
| `Endpoint` | Properties applied when provisioning an endpoint. `toEndpointProperties()` and `toEndpointProperties(AccessType override)` build the JCSMP object; the override is how an exchange pattern imposes exclusive or non-exclusive access. |
| `DeadMessageQueue` | The VPN's dead message queue. Its `toEndpointProperties()` forces `respectsTTL` to `false`, which the broker requires. |
| `DispatchMode` | `INLINE` or `EXECUTOR`. |
| `AccessType` | `EXCLUSIVE`, `NONEXCLUSIVE`; `value()` yields the JCSMP constant. |
| `Permission` | `NONE`, `READ_ONLY`, `CONSUME`, `MODIFY_TOPIC`, `DELETE`; `value()` yields the JCSMP constant. |

---

## DefaultSolaceMessageListenerContainer

Binds an endpoint to the broker and dispatches messages. The workhorse of the package.

**Constructor:** `(SolaceSessionFactory, SolaceListenerEndpoint, ContainerProperties, String instanceId)`

| Setter | Purpose |
| :--- | :--- |
| `setupMessageListener(SolaceMessageListener)` | The listener to invoke. |
| `setErrorHandler(SolaceListenerErrorHandler)` | Defaults to logging. |
| `setTransactionManager(SolaceTransactionManager)` | Required when transactional. |
| `setTaskExecutor(AsyncTaskExecutor)` | Required under `EXECUTOR` dispatch. |

| Method | Description |
| :--- | :--- |
| `start()` | Validates configuration, binds the endpoint, starts flows. Rolls back everything it opened if any step fails. |
| `stop()` / `stop(Runnable)` | Halts delivery, drains invokers within `shutdownTimeout`, then closes flows and sessions. |
| `isRunning()`, `isAutoStartup()`, `getPhase()` | `SmartLifecycle`. |
| `getResolvedQueueName()` | The physical endpoint name, available once started — for a temporary queue this is the broker-assigned `#P2P/QTMP/...` name. |

### Startup validation

* transactional requires a `SolaceTransactionManager` and a queue-based endpoint mode;
* transactional requires `concurrency <= maxTransactedSessionsPerConnection`;
* `EXECUTOR` dispatch requires a task executor and forbids `transactional`;
* binding several flows to an **exclusive** endpoint logs a warning naming the cause.

### Binding order

1. provision the DMQ, then the endpoint (durable modes only);
2. create the flows, **stopped** — this is what materialises a temporary queue;
3. add topic subscriptions;
4. start the flows.

Subscribing before a flow binds fails with `503 Unknown Queue` on a temporary endpoint; starting
flows before subscribing would open a delivery window with no subscriptions.

### Idempotence

Provisioning tolerates `ENDPOINT_ALREADY_EXISTS` and reports `ENDPOINT_PROPERTY_MISMATCH` as a
warning — the broker never reconfigures an existing endpoint, so silence would make a queue look
configured when it is not. Subscriptions tolerate `SUBSCRIPTION_ALREADY_PRESENT`, which is the normal
state on every restart after the first.

### Dispatch

`INLINE` invokes the listener on the JCSMP delivery thread. `EXECUTOR` runs one `FlowInvoker` task
per flow on the task executor; the delivery thread only hands the message to a **bounded blocking**
queue, so back-pressure reaches the broker's transport window instead of being absorbed by unbounded
buffering.

## ContainerKeepAlive

Package-private. A reference-counted non-daemon thread held while at least one container runs.
Every JCSMP thread is a daemon thread, so without it a consumer-only application shuts down
immediately after starting. Toggled by `solace.listener.keep-alive`.

---

## Listener adapters

### AbstractSolaceListenerAdapter

Base class: converts the inbound message and publishes whatever the listener returns, the way
`@KafkaListener` does with `@SendTo`.

| Member | Description |
| :--- | :--- |
| `payloadType` | Type the body is converted into. Default `Object` (the raw message). |
| `replyTemplate` | Template used for replies; joins the container's transaction automatically. |
| `replyDestination` | Fixed reply destination; when unset, the request's `replyTo` is used. |
| `convertPayload(BytesXMLMessage)` | Convert using `payloadType`. |
| `toRecord(Object, BytesXMLMessage, Map)` | Build a `SolaceRecord`. |
| `handleResult(Object, BytesXMLMessage)` | Resolve the reply destination (message header → configured → request `replyTo`), copy the correlation id and the `instanceId` / `requestSendTime` properties, publish. Logs and discards when there is nowhere to reply. |

### MethodSolaceListenerAdapter

Invokes a `@SolaceListener` method through an `InvocableHandlerMethod`. Supported parameters:
the converted payload, `Message<?>`, `SolaceRecord<?>`, the raw `BytesXMLMessage`, and
`@Payload` / `@Header` / `@Headers`.

### RecordSolaceListenerAdapter&lt;T,R&gt;

Adapts a plain `Function<SolaceRecord<T>, R>`, for listeners registered programmatically rather than
by annotation. A non-null return value is published as a reply.

---

## SolaceListenerContainerFactory

Functional interface: `createListenerContainer(SolaceListenerEndpoint)`.

## DefaultSolaceListenerContainerFactory

The analogue of `ConcurrentKafkaListenerContainerFactory`, registered as
`solaceListenerContainerFactory`.

**Constructor:** `(SolaceSessionFactory, SolaceMessageConverter, SolaceHeaderMapper,
InstanceIdProvider, ContainerProperties)`

Setters: `transactionManager`, `replyTemplate`, `errorHandler`, `taskExecutor`.

`createListenerContainer` builds a `MethodSolaceListenerAdapter` for annotated methods, or uses the
endpoint's own listener, then wires the container with the shared collaborators.

---

## SolaceListenerEndpointRegistry

Holds every container and drives its lifecycle — the counterpart of `KafkaListenerEndpointRegistry`.
Implements `SmartLifecycle` and `DisposableBean`.

| Method | Description |
| :--- | :--- |
| `registerListenerContainer(endpoint, factory)` | Create and register a container. Rejects a duplicate id. Starts it immediately if the registry is already running and the container is auto-startup. |
| `getListenerContainer(String id)` | Look one up — the handle for starting or stopping a listener at runtime. |
| `getListenerContainerIds()` / `getListenerContainers()` | Enumerate. |
| `start()` / `stop()` / `isRunning()` / `getPhase()` / `destroy()` | Lifecycle; containers start in phase order. |

## SolaceListenerAnnotationBeanPostProcessor

Finds `@SolaceListener` methods on singleton beans and turns each into a container, mirroring
`KafkaListenerAnnotationBeanPostProcessor`.

* `postProcessAfterInitialization` collects annotated methods (target class, so proxies are handled);
* `afterSingletonsInstantiated` builds a `DefaultMessageHandlerMethodFactory`, resolves placeholders,
  derives the payload type, applies pattern defaults, and registers each endpoint;
* the container factory is resolved by name, falling back to a unique bean of the right type with a
  diagnostic message when neither is present.

Payload type derivation: the first parameter that is not `@Header`/`@Headers`, `MessageHeaders`, or
`BytesXMLMessage`, unwrapping `Message<T>` and `SolaceRecord<T>`.

## SolaceListenerConfigUtils

Well-known bean names: the annotation processor, the endpoint registry, and
`solaceListenerContainerFactory`.
