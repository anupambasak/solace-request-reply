# 15. Class reference

Every public type in the library, what it is for, and where it is explained in depth.

Base package `cris.prs.messaging.solace`, except the auto-configuration, which is deliberately
outside it — see [4.9](04-spring-integration.md#49-why-the-auto-configuration-package-is-separate).

---

## 15.1 `core` — sessions, sending, conversion

| Type | Kind | Purpose |
| :--- | :--- | :--- |
| `SolaceOperations<T>` | interface | The send contract: six `send` overloads plus `executeInTransaction`. Nested `TransactionCallback<T,R>` with `R doInSolace(SolaceOperations<T>)`. Counterpart of `KafkaOperations`. |
| `SolaceTemplate<T>` | class | The implementation. Holds the delivery defaults, converts, maps headers, picks the transaction-aware producer. `createMessage` and `producer()` are `protected` for subclassing. |
| `SolaceSessionFactory` | interface | `getSharedSession`, `createSession`, `getSharedProducer`, `getProducer(session)`, `createTransactedSession()`, `createTransactedSession(session)`, `closeSession`. |
| `DefaultSolaceSessionFactory` | class | Wraps `SpringJCSMPFactory`. One lazy shared session, producers cached per session, every created session tracked and closed on `destroy()`. Nested `LoggingPublishEventHandler` logs async publish outcomes. |
| `SolaceMessageConverter` | interface | `toMessage(Object)` / `fromMessage(BytesXMLMessage, Class)`. |
| `JacksonSolaceMessageConverter` | class | JSON by default; reuses the application's `ObjectMapper`. Reads the **binary attachment** first, the XML content part as fallback. |
| `SolaceHeaderMapper` | interface | `fromHeaders(Map, XMLMessage)` / `toHeaders(BytesXMLMessage)`. |
| `DefaultSolaceHeaderMapper` | class | Maps the `solace_*` fields, copies everything else to SDT user properties. Statics: `toDestination(Object)` (`queue:` prefix ⇒ queue), `sanitize(Map)`, constant `QUEUE_PREFIX`. |
| `SolaceHeaders` | final class | The well-known header names. See [12.2](12-conversion-and-headers.md#122-solaceheadermapper). |
| `SolaceRecord<T>` | class | Payload plus destination, correlation id, replyTo, headers, raw message, and `isRedelivered()`. |
| `EndpointMode` | enum | `DURABLE_QUEUE`, `NON_DURABLE_QUEUE`, `DIRECT`; `isQueueBased()`. |
| `ExchangePattern` | enum | `PUBLISH_SUBSCRIBE`, `POINT_TO_POINT`, `REQUEST_REPLY`. |
| `SolaceMessagingException` | class | `NestedRuntimeException`. Every JCSMP checked exception is translated to this. |

→ [8. Producing messages](08-producing-messages.md), [12. Conversion and headers](12-conversion-and-headers.md)

---

## 15.2 `listener` — consuming

| Type | Kind | Purpose |
| :--- | :--- | :--- |
| `SolaceListenerEndpoint` | class | Value object describing what to consume. Overrides are boxed types so "unset" is distinguishable. `applyPatternDefaults()`, `resolveQueueName(instanceId)`, `resolveTopics(instanceId)`. |
| `SolaceListenerContainerFactory` | interface | `createListenerContainer(SolaceListenerEndpoint)`. |
| `DefaultSolaceListenerContainerFactory` | class | Builds containers from the shared collaborators and the default `ContainerProperties`. Settable: `transactionManager`, `replyTemplate`, `errorHandler`, `taskExecutor`. |
| `SolaceMessageListenerContainer` | interface | `SmartLifecycle` + `getListenerId()` + `setupMessageListener(...)`. |
| `DefaultSolaceMessageListenerContainer` | class | Provisioning, flow binding, subscriptions, dispatch, transactions, shutdown. `getResolvedQueueName()` gives the physical endpoint name once started. |
| `ContainerProperties` | class | Every container setting; the type `solace.listener.*` binds to. Nested: `Endpoint`, `DeadMessageQueue`, and the enums `DispatchMode`, `AccessType`, `Permission`. |
| `SolaceListenerEndpointRegistry` | class | Holds containers by id; `SmartLifecycle` and `DisposableBean`. `registerListenerContainer`, `getListenerContainer(id)`, `getListenerContainerIds()`, `getListenerContainers()`. |
| `SolaceListenerAnnotationBeanPostProcessor` | class | Finds `@SolaceListener` methods, resolves placeholders, builds endpoints, registers containers. `BeanPostProcessor` + `SmartInitializingSingleton` + `BeanFactoryAware` + `Ordered`. |
| `SolaceListenerConfigUtils` | final class | The three well-known infrastructure bean names. |
| `SolaceMessageListener` | interface | `void onMessage(BytesXMLMessage) throws Exception`. |
| `AbstractSolaceListenerAdapter` | abstract class | Shared conversion, `SolaceRecord` construction, and `handleResult` — the reply-publishing logic. |
| `MethodSolaceListenerAdapter` | class | Invokes an `InvocableHandlerMethod`. What `@SolaceListener` uses. |
| `RecordSolaceListenerAdapter<T,R>` | class | Invokes a `Function<SolaceRecord<T>, R>`, for programmatic registration. |
| `SolaceListenerErrorHandler` | interface | `void handleError(BytesXMLMessage, Exception)`. |
| `ContainerKeepAlive` | package-private final class | Reference-counted non-daemon thread that keeps a listener-only JVM alive. |

→ [9. Consuming messages](09-consuming-messages.md), [6. Annotations](06-annotations.md)

---

## 15.3 `requestreply`

| Type | Kind | Purpose |
| :--- | :--- | :--- |
| `ReplyingSolaceTemplate` | class | Extends `SolaceTemplate<Object>`; `SmartLifecycle`, `InitializingBean`, `DisposableBean`. Three `sendAndReceive` overloads, `getPendingCount()`, `protected onReply(BytesXMLMessage)` for tracing hooks. |
| `RequestReplyFuture<R>` | class | `CompletableFuture<R>` plus correlation id, request and reply destinations, send/receive times and `getLatency()`. |
| `ReplyEndpointSpec` | class | Describes one reply destination. `SolaceProperties.RequestReply` extends it, so YAML and code use the same object. |
| `ReplyingSolaceTemplateFactory` | class | `create(ReplyEndpointSpec)` builds a template *and* its reply container. `createReplyContainer` is `protected`. |
| `SolaceReplyTimeoutException` | class | Extends `SolaceMessagingException`. Also used to fail outstanding futures at shutdown. |

→ [10. Request-reply](10-request-reply.md)

---

## 15.4 `transaction`

| Type | Kind | Purpose |
| :--- | :--- | :--- |
| `SolaceTransactionManager` | class | `AbstractPlatformTransactionManager` + `ResourceTransactionManager`. Makes `@Transactional` and `TransactionTemplate` work. |
| `SolaceResourceHolder` | class | `ResourceHolderSupport` wrapping one `TransactedSession` and its producer. The `externallyManaged` flag stops the manager closing a session the container owns. |
| `SolaceTransactionUtils` | final class | `getResourceHolder`, `getActiveResourceHolder`, `bindResourceHolder`, `unbindResourceHolder` over `TransactionSynchronizationManager`. |

→ [11. Transactions](11-transactions.md)

---

## 15.5 `support`

| Type | Kind | Purpose |
| :--- | :--- | :--- |
| `InstanceIdProvider` | interface | `String getInstanceId()`. |
| `HostnameInstanceIdProvider` | class | `HOSTNAME` → `POD_NAME` → local host → random, then sanitised. Static `sanitize(String)`. |
| `ReplyDestinationResolver` | final class | `resolveTopic(prefix, appendInstanceId, instanceId)` and `resolveQueueBaseName(configuredQueue, topicPrefix)`. |

→ [13. Multi-instance](13-multi-instance.md)

---

## 15.6 `annotation`

| Type | Purpose |
| :--- | :--- |
| `@EnableSolace` | Imports `SolaceBootstrapConfiguration`. Not needed in a Boot application. |
| `@SolaceListener` | Method-level listener declaration. Fourteen attributes, all `String` for placeholder support. |

→ [6. Annotations](06-annotations.md)

---

## 15.7 `cris.prs.solace.autoconfigure`

| Type | Purpose |
| :--- | :--- |
| `SolaceAutoConfiguration` | `@AutoConfiguration(afterName = SolaceJavaAutoConfiguration)`. Declares every bean, each conditional. |
| `SolaceAnnotationDrivenConfiguration` | `@Configuration` carrying `@EnableSolace`, conditional on the post-processor being absent. Imported by the auto-configuration. |
| `SolaceBootstrapConfiguration` | `ImportBeanDefinitionRegistrar` registering the post-processor and the registry as `ROLE_INFRASTRUCTURE` beans. |
| `SolaceProperties` | `@ConfigurationProperties("solace")`. Nested `Template`, `Listener extends ContainerProperties`, `RequestReply extends ReplyEndpointSpec`. |

→ [4. Spring integration](04-spring-integration.md), [5. Configuration](05-configuration.md)

---

## 15.8 Beans in a running context

| Bean name | Type | Condition |
| :--- | :--- | :--- |
| `solaceInstanceIdProvider` | `InstanceIdProvider` | missing bean |
| `solaceMessageConverter` | `SolaceMessageConverter` | missing bean |
| `solaceHeaderMapper` | `SolaceHeaderMapper` | missing bean |
| `solaceSessionFactory` | `SolaceSessionFactory` | missing bean |
| `solaceTransactionManager` | `SolaceTransactionManager` | missing bean |
| `solaceTemplate` | `SolaceTemplate<Object>` | missing bean **by name**; `@Primary` |
| `solaceListenerTaskExecutor` | `AsyncTaskExecutor` | missing bean by name |
| `solaceListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | missing bean by name |
| `replyingSolaceTemplateFactory` | `ReplyingSolaceTemplateFactory` | missing bean |
| `replyingSolaceTemplate` | `ReplyingSolaceTemplate` | missing bean **by name** + `solace.request-reply.enabled` ≠ false |
| *(infrastructure)* | `SolaceListenerAnnotationBeanPostProcessor` | registered by `@EnableSolace` |
| *(infrastructure)* | `SolaceListenerEndpointRegistry` | registered by `@EnableSolace` |

---

## 15.9 Javadoc

```bash
gradle :solace-library:javadoc      # build/docs/javadoc/index.html
```

The task runs with `-Xdoclint:all`, so a broken reference fails the build. One rule when editing:
**never `{@link}` a Lombok-generated accessor.** Lombok generates after javadoc reads the source, so
`{@link ContainerProperties#isKeepAlive()}` cannot be resolved and doclint reports it as an error.
Use `{@code …}` for those.

---

**Next:** [16. Operations](16-operations.md)
