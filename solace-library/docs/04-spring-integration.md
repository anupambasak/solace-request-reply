# 4. Spring integration

This page is the complete account of how the library plugs into Spring and Spring Boot: which
framework contracts it implements, in what order they fire, what each one is responsible for, and
what happens when you override any of it.

---

## 4.1 The Spring contracts this library implements

| Contract | Implemented by | Purpose |
| :--- | :--- | :--- |
| `@AutoConfiguration` | `SolaceAutoConfiguration` | Contributes every bean, after the Solace starter |
| `@ConfigurationProperties` | `SolaceProperties` | Binds `solace.*` from the `Environment` |
| `ImportBeanDefinitionRegistrar` | `SolaceBootstrapConfiguration` | Registers the two infrastructure beans |
| `BeanPostProcessor` | `SolaceListenerAnnotationBeanPostProcessor` | Discovers `@SolaceListener` methods |
| `SmartInitializingSingleton` | *(same class)* | Registers containers once all singletons exist |
| `BeanFactoryAware`, `Ordered` | *(same class)* | Placeholder resolution, and running last |
| `SmartLifecycle` | `SolaceListenerEndpointRegistry`, `DefaultSolaceMessageListenerContainer`, `ReplyingSolaceTemplate` | Phased start/stop |
| `InitializingBean` | `ReplyingSolaceTemplate` | Wires the reply listener |
| `DisposableBean` | `ReplyingSolaceTemplate`, `SolaceListenerEndpointRegistry`, `DefaultSolaceSessionFactory` | Deterministic cleanup |
| `PlatformTransactionManager` (via `AbstractPlatformTransactionManager`) | `SolaceTransactionManager` | `@Transactional`, `TransactionTemplate` |
| `ResourceTransactionManager` | *(same class)* | Exposes the session factory as the transaction resource key |
| `ResourceHolderSupport` | `SolaceResourceHolder` | Thread-bound transactional resource |
| `AsyncTaskExecutor` (consumer) | `DefaultSolaceMessageListenerContainer` | `EXECUTOR` dispatch |
| `MessageHandlerMethodFactory` / `InvocableHandlerMethod` (consumer) | `MethodSolaceListenerAdapter` | Argument resolution on listener methods |
| `NestedRuntimeException` | `SolaceMessagingException` | Consistent unchecked exception translation |
| `ObjectProvider` (consumer) | `SolaceAutoConfiguration` | Optional `ObjectMapper`, optional metrics collaborators |
| `HealthIndicator` (Actuator) | `SolaceHealthIndicator` | `/actuator/health/solace` |
| `MeterRegistry` (Micrometer, consumer) | `MicrometerSolace*Metrics`, `SolaceMetricsBinder` | Meters for listeners and request-reply |

---

## 4.2 Auto-configuration

### Registration

The library ships a Spring Boot auto-configuration import file:

```
src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

containing one line — the fully-qualified name of `SolaceAutoConfiguration`. That is the modern
replacement for `spring.factories`, and it is what makes the library work with nothing but the
dependency on the classpath.

### Class-level conditions and ordering

```java
@AutoConfiguration(afterName = {
        "com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"})
@ConditionalOnClass({JCSMPSession.class, SpringJCSMPFactory.class})
@EnableConfigurationProperties(SolaceProperties.class)
@Import({SolaceAnnotationDrivenConfiguration.class, SolaceObservabilityConfiguration.class})
public class SolaceAutoConfiguration { … }
```

- **`afterName`** — ordering constraints, expressed as *strings* so the library needs none of those
  classes on its compile classpath. The first guarantees `SpringJCSMPFactory` has been contributed
  before any condition here is evaluated; the two metrics entries guarantee that a `MeterRegistry`,
  if the application has one, exists before the observability configuration decides whether to
  instrument anything.
- **`@ConditionalOnClass`** — the whole configuration disappears if JCSMP is absent.
- **`@EnableConfigurationProperties`** — binds and registers `SolaceProperties` without needing
  `@ConfigurationPropertiesScan` in the application.
- **`@Import`** — pulls in the annotation-driven configuration, which is what turns
  `@SolaceListener` on. This is why applications do not write `@EnableSolace` themselves.

There is deliberately **no class-level `@ConditionalOnBean`**. Bean-presence conditions on an
auto-configuration class are order-sensitive and evaluate against a partially-populated bean factory;
using one here produced a context where the whole configuration silently vanished. `afterName` plus
`@ConditionalOnClass` expresses the same intent reliably.

### The beans, and the conditions on each

| Bean name | Type | Condition | Notes |
| :--- | :--- | :--- | :--- |
| `solaceInstanceIdProvider` | `InstanceIdProvider` | missing bean | `HostnameInstanceIdProvider`, seeded from `solace.instance-id` |
| `solaceMessageConverter` | `SolaceMessageConverter` | missing bean | Jackson; reuses the context's `ObjectMapper` via `ObjectProvider` if there is one |
| `solaceHeaderMapper` | `SolaceHeaderMapper` | missing bean | `DefaultSolaceHeaderMapper` |
| `solaceSessionFactory` | `SolaceSessionFactory` | missing bean | Wraps `SpringJCSMPFactory` |
| `solaceTransactionManager` | `SolaceTransactionManager` | missing bean | Registered whether or not anything is transactional |
| `solaceTemplate` | `SolaceTemplate<Object>` | missing bean **by name** | `@Primary`; defaults from `solace.template.*` |
| `solaceListenerTaskExecutor` | `AsyncTaskExecutor` | missing bean by name | `SimpleAsyncTaskExecutor`, only used by `EXECUTOR` dispatch |
| `solaceListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | missing bean by name | Name is `SolaceListenerConfigUtils.DEFAULT_SOLACE_LISTENER_CONTAINER_FACTORY_BEAN_NAME`. Given the application's `SolaceFlowListener` and `SolaceListenerErrorHandler` beans, when there are any |
| `replyingSolaceTemplateFactory` | `ReplyingSolaceTemplateFactory` | missing bean | Builds additional reply destinations |
| `replyingSolaceTemplate` | `ReplyingSolaceTemplate` | missing bean **by name** + `solace.request-reply.enabled` ≠ `false` | Built from `solace.request-reply.*` |

Plus, from the imported `SolaceSchemaRegistryConfiguration` — only when `solace.schema-registry.url` is
set, and imported **first** so that its converter is registered before the core
`solaceMessageConverter` evaluates `@ConditionalOnMissingBean`:

| Bean name | Type | Condition | Notes |
| :--- | :--- | :--- | :--- |
| `solaceSchemaCodecs` | `SchemaCodecs` | missing bean | One Apicurio codec per enabled format (Avro, Protobuf, JSON Schema). Fails startup, naming the fix, when a format's Apicurio module is missing. Never contacts the registry |
| `solaceMessageConverter` | `SchemaRegistrySolaceMessageConverter` | missing `SolaceMessageConverter` | Replaces the Jackson converter, which it keeps as its fallback |
| `solaceSchemaRegistryErrorHandler` | `SchemaRegistryErrorHandler` | missing `SolaceListenerErrorHandler` | Rejects non-retryable schema failures |

It has no `@ConditionalOnClass` on the Apicurio jars, on purpose: a configured registry URL with a missing
jar should fail, not silently fall back to plain JSON. See [19](19-schema-registry.md).

Plus, from the imported `SolaceObservabilityConfiguration`:

| Bean name | Type | Condition | Notes |
| :--- | :--- | :--- | :--- |
| `solaceListenerMetrics` | `SolaceListenerMetrics` | missing bean + `MeterRegistry` bean present + `solace.metrics.enabled` ≠ `false` | Injected into the container factory |
| `solaceRequestReplyMetrics` | `SolaceRequestReplyMetrics` | as above | Injected into `ReplyingSolaceTemplateFactory` |
| `solaceMetricsBinder` | `SolaceMetricsBinder` | as above | Registers the state gauges |
| `solaceHealthIndicator` | `SolaceHealthIndicator` | missing bean **by name** + Actuator on the classpath + `solace.health.enabled` ≠ `false` | Contributes `/actuator/health/solace` |

### `@ConditionalOnMissingBean`: by type, or by name?

This distinction matters more than it looks.

A bare `@ConditionalOnMissingBean` matches **by type**. That is right for beans an application
either accepts or replaces — a converter, a header mapper, a session factory.

It is **wrong** for beans an application is expected to have *several* of. `solaceTemplate` and
`replyingSolaceTemplate` therefore match by name:

```java
@Bean
@Primary
@ConditionalOnMissingBean(name = "solaceTemplate")
public SolaceTemplate<Object> solaceTemplate(…) { … }

@Bean
@ConditionalOnMissingBean(name = "replyingSolaceTemplate")
@ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public ReplyingSolaceTemplate replyingSolaceTemplate(…) { … }
```

Two reasons:

1. `ReplyingSolaceTemplate extends SolaceTemplate<Object>`. Enabling request-reply would otherwise
   make the base template back off, because a bean of its type now exists.
2. Declaring a second `ReplyingSolaceTemplate` — to give one service its own reply destination — is
   a supported, documented thing to do. With by-type matching, doing so removed the auto-configured
   template and every `@Qualifier("replyingSolaceTemplate")` injection failed.

**The rule:** a library bean that an application may legitimately *add to* must condition on its
name. An extra bean of the same type then adds; only a bean of the same *name* replaces.

### Why `solaceTemplate` is `@Primary`

Because `ReplyingSolaceTemplate` is a `SolaceTemplate<Object>`, an unqualified injection of
`SolaceTemplate` is ambiguous as soon as request-reply is on. `@Primary` on the plain template makes
the common case work. Note there is deliberately **no** `@Primary` on `replyingSolaceTemplate`: a
second primary in the same assignable hierarchy would break the first. Applications with more than
one `ReplyingSolaceTemplate` qualify explicitly.

---

## 4.3 Configuration property binding

`SolaceProperties` is a plain `@ConfigurationProperties(prefix = "solace")` class bound by Spring
Boot's relaxed binder, so `maxRedeliveryCount`, `max-redelivery-count` and `MAX_REDELIVERY_COUNT`
are all the same property.

Its structure reuses the runtime types rather than duplicating them:

```java
@Data @ConfigurationProperties(prefix = "solace")
public class SolaceProperties {
    private String instanceId;
    private final Template     template     = new Template();
    private final Listener     listener     = new Listener();       // extends ContainerProperties
    private final RequestReply requestReply = new RequestReply();   // extends ReplyEndpointSpec
}
```

`Listener extends ContainerProperties` and `RequestReply extends ReplyEndpointSpec` so that a
YAML-configured container and a hand-built one are described by exactly the same object. There is no
mapping layer to fall out of sync, and a new container property becomes configurable the moment it
is added to `ContainerProperties`.

`spring-boot-configuration-processor` runs as an annotation processor, so these properties get
IDE completion and javadoc-sourced descriptions in `spring-configuration-metadata.json`.

---

## 4.4 `@SolaceListener` discovery: the BeanPostProcessor

`SolaceListenerAnnotationBeanPostProcessor` implements `BeanPostProcessor`,
`SmartInitializingSingleton`, `BeanFactoryAware` and `Ordered`. It is the exact analogue of
`KafkaListenerAnnotationBeanPostProcessor`.

### Phase 1 — `postProcessAfterInitialization`

Runs for every bean in the context. It returns the bean unchanged; the only side effect is
collection:

```java
Class<?> targetClass = AopUtils.getTargetClass(bean);
MethodIntrospector.selectMethods(targetClass, method ->
        AnnotatedElementUtils.findMergedAnnotation(method, SolaceListener.class))
    .forEach((method, annotation) -> listenerMethods.add(...));
```

- `AopUtils.getTargetClass` unwraps proxies, so a `@Transactional` or `@Async` listener bean is
  still scanned correctly.
- `AnnotatedElementUtils.findMergedAnnotation` supports **meta-annotations**: you can define your
  own `@OrderListener` annotated with `@SolaceListener` and attribute aliases resolve normally.
- `getOrder()` returns `LOWEST_PRECEDENCE` so this post-processor sees beans fully initialised, after
  every other post-processor has had its turn.

### Phase 2 — `afterSingletonsInstantiated`

Nothing is registered during phase 1. Registration is deferred until every singleton exists, which
is what makes the container factory and the registry safe to look up without creating circular
references.

For each collected method:

```
resolve() every String attribute through the BeanFactory's embedded value resolver
    → property placeholders such as "${app.concurrency:10}" work in ALL attributes
build a SolaceListenerEndpoint from the resolved values
applyPatternDefaults()               ← last, so explicit attributes always win
resolvePayloadType(method)           ← what the body is converted into
handlerMethodFactory.createInvocableHandlerMethod(bean, method)
resolveContainerFactory(name)        ← by name, then by unique type
assert topics or a queue are present
registry.registerListenerContainer(endpoint, factory)
```

Because every `@SolaceListener` attribute is declared as `String`, *all* of them are
placeholder-capable — including numeric and boolean ones:

```java
@SolaceListener(
        queue        = "${app.orders.queue}",
        group        = "${app.orders.group:workers}",
        concurrency  = "${app.orders.concurrency:5}",
        transactional= "${app.orders.transactional:true}")
```

### Payload type derivation

`resolvePayloadType` walks the method parameters and picks the first that is not framework
machinery:

- skipped: `@Header`, `@Headers`, `MessageHeaders`, `BytesXMLMessage`;
- `Message<T>` or `SolaceRecord<T>` → the generic `T`;
- anything else → that parameter's type;
- nothing matched → `Object`.

That single type is what `SolaceMessageConverter.fromMessage` is asked for. A listener taking only
headers and the raw message therefore performs no body conversion at all.

### Container factory resolution, and its error message

Looked up by the `containerFactory` attribute, defaulting to
`solaceListenerContainerFactory`. If no such bean exists, it falls back to a *unique* bean of type
`SolaceListenerContainerFactory`; if that is also ambiguous or absent it throws with a message that
names the actual likely cause:

> No SolaceListenerContainerFactory named 'solaceListenerContainerFactory' is available … The Solace
> auto-configuration did not run: check that `solace.java.host` is configured and that the
> application does not component scan the `org.cris.prs.solace.autoconfigure` package.

That fallback exists because an application which component-scans the auto-configuration package
ends up with the post-processor but not the factory — a confusing state that deserves a plain
explanation rather than a `NoSuchBeanDefinitionException`.

---

## 4.5 Listener method signatures

`MethodSolaceListenerAdapter` invokes through Spring's `InvocableHandlerMethod`, created by a
`DefaultMessageHandlerMethodFactory`. Every argument resolver that factory registers is therefore
available, and the adapter supplies three "root" objects to resolve from:

```java
Object result = handlerMethod.invoke(springMessage, rawMessage, solaceRecord);
```

Which gives you:

| Parameter | Resolved from |
| :--- | :--- |
| the payload type, e.g. `Order order` | `PayloadMethodArgumentResolver` over the Spring `Message` |
| `Message<Order>` | the Spring `Message` directly |
| `SolaceRecord<Order>` | passed as a root object |
| `BytesXMLMessage` | passed as a root object |
| `@Header("solace_correlationId") String id` | `HeaderMethodArgumentResolver` |
| `@Headers Map<String,Object> headers` | `HeadersMethodArgumentResolver` |
| `MessageHeaders headers` | ditto |

Any combination, in any order:

```java
@SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", topics = "pricing/quote")
public Quote quote(PriceRequest request,
                   @Header(SolaceHeaders.CORRELATION_ID) String correlationId,
                   @Header(name = "tenant", required = false) String tenant,
                   SolaceRecord<PriceRequest> record,
                   BytesXMLMessage raw) { … }
```

`SolaceHeaders.RAW_MESSAGE` is also placed into the header map, so the raw message is reachable
either way.

**The return value is the reply.** A non-void return publishes; `void` or `null` publishes nothing.
That is the entire request-reply opt-in on the responder side.

To customise argument resolution, declare your own `MessageHandlerMethodFactory`-configured
post-processor — or, more simply, take a `SolaceRecord<T>` and read what you need from it.

---

## 4.6 `@EnableSolace` and the bootstrap registrar

```java
@Target(TYPE) @Retention(RUNTIME) @Documented
@Import(SolaceBootstrapConfiguration.class)
public @interface EnableSolace { }
```

`SolaceBootstrapConfiguration` is an `ImportBeanDefinitionRegistrar` that registers two beans, each
marked `ROLE_INFRASTRUCTURE` so they are hidden from application-facing bean reports:

| Bean name (from `SolaceListenerConfigUtils`) | Type |
| :--- | :--- |
| `SOLACE_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME` | `SolaceListenerAnnotationBeanPostProcessor` |
| `SOLACE_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME` | `SolaceListenerEndpointRegistry` |

Both registrations are guarded by `containsBeanDefinition`, so importing `@EnableSolace` twice — for
instance once through auto-configuration and once by hand — is harmless.

`SolaceAnnotationDrivenConfiguration` is a `@Configuration(proxyBeanMethods = false)` class carrying
`@EnableSolace` and `@ConditionalOnMissingBean(SolaceListenerAnnotationBeanPostProcessor.class)`.
It is imported by the auto-configuration, which is why Boot applications get annotation-driven
listeners for free, and why declaring `@EnableSolace` yourself does not double-register anything.

Use `@EnableSolace` explicitly when you are not on Spring Boot, or in a test that assembles a
context from `@Configuration` classes rather than through auto-configuration.

---

## 4.7 Lifecycle and phases

Three bean types implement `SmartLifecycle`:

| Bean | Phase | On start | On stop |
| :--- | :--- | :--- | :--- |
| `SolaceListenerEndpointRegistry` | `Integer.MAX_VALUE - 100` | starts every registered container | stops every container |
| `DefaultSolaceMessageListenerContainer` | `containerProperties.phase`, default `MAX - 100` | provisions, binds flows, subscribes | closes flows and sessions |
| `ReplyingSolaceTemplate` | `MAX - 90` | starts its reply container, starts the timeout scheduler | stops the container, fails outstanding futures |

Spring starts ascending and stops descending, so:

- **on start**, listener containers are consuming *before* any `ReplyingSolaceTemplate` may send —
  a reply cannot beat its correlation entry into existence;
- **on stop**, the requester stops first and fails its outstanding futures, so nothing is left
  waiting on replies that can no longer arrive.

Both phases are late (`MAX - 100`, `MAX - 90`) so that messaging starts after the rest of the
application — data sources, caches, web server — is ready, and stops before them.

`isAutoStartup()` is honoured per container (`solace.listener.auto-startup`, or the annotation's
`autoStartup`). A container with `autoStartup=false` is registered but idle; start it later through
the registry:

```java
registry.getListenerContainer("orders").start();
```

`SolaceListenerEndpointRegistry` also implements `DisposableBean` and stops everything on context
close, which covers the case of a context that is destroyed without a lifecycle stop.

---

## 4.8 Transaction integration

`SolaceTransactionManager` extends `AbstractPlatformTransactionManager` and implements
`ResourceTransactionManager`, which is all Spring needs to drive it from `@Transactional`,
`TransactionTemplate`, or `TransactionSynchronizationManager` directly.

| Template method | What it does |
| :--- | :--- |
| `getResourceFactory()` | Returns the `SolaceSessionFactory` — the key resources are bound under |
| `doGetTransaction()` | Builds a transaction object holding the currently bound `SolaceResourceHolder`, if any |
| `isExistingTransaction()` | True when a holder is already bound and active — supports `PROPAGATION_REQUIRED` |
| `doBegin()` | Creates a `TransactedSession`, wraps it in a `SolaceResourceHolder`, binds it to the thread |
| `doCommit()` | `TransactedSession.commit()` |
| `doRollback()` | `TransactedSession.rollback()` |
| `doSetRollbackOnly()` | Marks the holder rollback-only |
| `doCleanupAfterCompletion()` | Unbinds and closes the session — unless it was externally managed |

The **externally managed** flag is how a transacted *listener flow* participates. The container
creates the `TransactedSession` itself (it must, because the flow is bound to it) and binds a holder
marked `externallyManaged = true` before invoking the `TransactionTemplate`. The transaction manager
then drives commit and rollback but does not close a session that the container owns and will reuse
for the next message.

Because `SolaceTemplate.send` consults `TransactionSynchronizationManager` through
`isTransactionActive()`, one `send` call behaves correctly in both worlds with no API difference.

`getResourceFactory()` returning the session factory also means Solace resources are keyed
independently of any `DataSource`, so a `@Transactional` method may sit inside a JDBC transaction
without interference — though the two commit separately, and are not atomic together. See
[11. Transactions](11-transactions.md).

---

## 4.9 Why the auto-configuration package is separate

`org.cris.prs.solace.autoconfigure` sits deliberately outside `cris.prs.messaging`.

The reference applications use `@ComponentScan("cris.prs.messaging")`. A component-scanned
`@AutoConfiguration` class is treated as an ordinary `@Configuration`: it is processed during the
normal configuration-class phase, *before* auto-configuration ordering applies. Its
`@ConditionalOnClass` still passes, but `@AutoConfiguration(afterName = …)` means nothing in that
position, so the class is evaluated before `SolaceJavaAutoConfiguration` has contributed
`SpringJCSMPFactory` — and every bean that depends on it is skipped without a word.

The observable symptom is the confusing one: `No bean named 'solaceListenerContainerFactory'
available`, from a `@SolaceListener` that looks perfectly correct.

The package boundary makes this structurally impossible. **Do not move the auto-configuration into a
package an application is likely to scan, and do not add a class-level `@ConditionalOnBean` to it.**

---

## 4.10 Overriding anything

Every bean is conditional, so declaring your own wins. Some examples:

```java
@Configuration
public class SolaceCustomisation {

    // Replace the converter (by type — this is a single-instance bean)
    @Bean
    SolaceMessageConverter solaceMessageConverter() {
        return new ProtobufSolaceMessageConverter();
    }

    // Replace the executor used by EXECUTOR dispatch (by name)
    @Bean
    AsyncTaskExecutor solaceListenerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setThreadNamePrefix("solace-");
        executor.initialize();
        return executor;
    }

    // ADD a second container factory, e.g. one with a custom error handler
    @Bean
    DefaultSolaceListenerContainerFactory auditListenerContainerFactory(
            SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
            SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds) {
        DefaultSolaceListenerContainerFactory factory =
                new DefaultSolaceListenerContainerFactory(sessionFactory, converter,
                        headerMapper, instanceIds, new ContainerProperties());
        factory.setErrorHandler((message, ex) -> audit.record(message, ex));
        return factory;
    }

    // ADD a second reply destination for one service
    @Bean
    ReplyingSolaceTemplate inventoryReplyingSolaceTemplate(ReplyingSolaceTemplateFactory factory) {
        ReplyEndpointSpec spec = new ReplyEndpointSpec();
        spec.setId("inventoryReplyContainer");
        spec.setReplyTopicPrefix("reply/inventory");
        return factory.create(spec);
    }
}
```

Then point one listener at the extra factory:

```java
@SolaceListener(queue = "audit", topics = "audit/>", containerFactory = "auditListenerContainerFactory")
public void onAudit(AuditEvent event) { … }
```

---

## 4.11 Observability wiring

`SolaceObservabilityConfiguration` is imported by the auto-configuration and split into two nested
`@Configuration` classes, each guarding its own dependency — an application may have Micrometer
without Actuator, or the reverse.

Three design points are worth knowing, because each was a choice with an alternative:

**The instrumentation SPIs carry no metrics types.** `SolaceListenerMetrics` (in `listener`) and
`SolaceRequestReplyMetrics` (in `requestreply`) are plain interfaces with no-op defaults and a `NO_OP`
constant. The container and the template call them unconditionally; when nothing is wired up they call
a no-op. So the `listener` and `requestreply` packages gain no dependency, and disabling metrics costs
literally nothing at runtime. The Micrometer implementations live in `observability`, which is the one
package in the library that touches a metrics library and Spring Boot Actuator.

**Both call sites are guarded.** Instrumentation must never be able to fail a message or a request, so
an exception thrown by a metrics implementation is logged at debug and swallowed.

**The gauges are a `SmartLifecycle`, not a `MeterBinder`.** Micrometer binds a `MeterBinder` when the
`MeterRegistry` bean is initialised. Listener containers are registered later, in the annotation
post-processor's `afterSingletonsInstantiated`, so a binder would frequently find none and silently
register no gauges. `SolaceMetricsBinder` starts at phase `Integer.MAX_VALUE` instead — after the
endpoint registry (`MAX - 100`) and every `ReplyingSolaceTemplate` (`MAX - 90`) — so everything it
samples is guaranteed to exist. Registration is idempotent, so a lifecycle restart does not duplicate
meters.

The health indicator takes `ObjectProvider<ReplyingSolaceTemplate>` rather than a single bean, so an
application with additional reply destinations gets all of them reported. It reads in-memory state
only and never contacts the broker.

`SolaceSessionFactory` gained `default boolean isHealthy()` for this — a *default* method, so a
custom session factory keeps compiling and is simply reported as healthy.

See [16.2](16-operations.md#162-micrometer-metrics) and [16.3](16-operations.md#163-actuator-health).

---

## 4.12 Other Spring ecosystem integrations

| Concern | How it behaves |
| :--- | :--- |
| **Spring Boot Actuator** | `SolaceHealthIndicator` contributes `/actuator/health/solace`, conditional on Actuator being present. See [16.3](16-operations.md#163-actuator-health). |
| **Spring Boot DevTools** | The restart classloader recreates the whole context; containers stop and flows close cleanly first. Temporary reply queues are dropped and recreated on each restart, which is what you want. |
| **`spring-boot-configuration-processor`** | Generates metadata for `solace.*`, so YAML completion and documentation work in IDEs. |
| **Spring WebFlux / Reactor** | `RequestReplyFuture` extends `CompletableFuture`, so `Mono.fromFuture(future)` is the whole bridge. The library imposes no blocking on the reactive path. |
| **Spring AOP** | Listener beans are scanned via `AopUtils.getTargetClass`, so proxied beans work. Note `@Transactional` on a listener *method* is redundant when the flow is already transacted, and can nest a second transaction. |
| **Spring test** | Nothing in the library requires a broker at bean-definition time, so a context that never starts the lifecycle (or sets `auto-startup: false`) can be built without one. Endpoint wiring is assertable — see [15. Class reference](15-class-reference.md) and the reference application's `ExchangePatternConfigurationTest`. |
| **Micrometer** | Ten meters covering listener throughput, listener latency, container state, and request-reply traffic — registered automatically when a `MeterRegistry` bean exists. See [16.2](16-operations.md#162-micrometer-metrics). |

---

**Next:** [5. Configuration reference](05-configuration.md)
