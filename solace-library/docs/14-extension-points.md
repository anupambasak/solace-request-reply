# 14. Extension points

Every collaborator is an interface with a default implementation registered
`@ConditionalOnMissingBean`. Replacing one is a matter of declaring a bean.

---

## 14.1 The map

| Interface | Default | Replace it to |
| :--- | :--- | :--- |
| `SolaceMessageConverter` | `JacksonSolaceMessageConverter` | Change the wire format — Protobuf, Avro, plain text, a schema registry |
| `SolaceHeaderMapper` | `DefaultSolaceHeaderMapper` | Change header naming, add tracing propagation, filter what crosses |
| `SolaceSessionFactory` | `DefaultSolaceSessionFactory` | Change session strategy — pooling, per-tenant connections |
| `InstanceIdProvider` | `HostnameInstanceIdProvider` | Change how this instance is named |
| `SolaceListenerErrorHandler` | logging lambda | Route failures to a dead-letter service, metrics, alerting — and decide each message's settlement outcome |
| `SolaceListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | Change how containers are built |
| `SolaceMessageListenerContainer` | `DefaultSolaceMessageListenerContainer` | Change consumption entirely |
| `SolaceMessageListener` | the adapters | Consume raw JCSMP messages |
| `AsyncTaskExecutor` (`solaceListenerTaskExecutor`) | `SimpleAsyncTaskExecutor` | Bound the pool, name threads, propagate MDC |
| `SolaceListenerMetrics` | Micrometer, or `NO_OP` | Report listener throughput and latency somewhere else |
| `SolaceRequestReplyMetrics` | Micrometer, or `NO_OP` | Report request-reply traffic somewhere else |
| `HealthIndicator` (`solaceHealthIndicator`) | `SolaceHealthIndicator` | Change what counts as healthy |

Two are conditioned **by name**, not by type, because applications are expected to declare additional
beans of the same type: `solaceTemplate` and `replyingSolaceTemplate`. See
[4.2](04-spring-integration.md#conditionalonmissingbean-by-type-or-by-name).

---

## 14.2 A custom converter

```java
public class TextSolaceMessageConverter implements SolaceMessageConverter {

    @Override
    public XMLMessage toMessage(Object payload) {
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        message.setText(payload.toString());
        return message;
    }

    @Override
    public Object fromMessage(BytesXMLMessage message, Class<?> targetType) {
        String text = message instanceof TextMessage textMessage
                ? textMessage.getText()
                : new String(readBody(message), StandardCharsets.UTF_8);
        return targetType == String.class ? text : parse(text, targetType);
    }
}
```

```java
@Bean
SolaceMessageConverter solaceMessageConverter() {
    return new TextSolaceMessageConverter();
}
```

One bean replaces conversion for the template, every listener, and request-reply. For *per-listener*
conversion, declare a second container factory instead (see 14.5).

## 14.3 A header mapper that propagates tracing

```java
public class TracingSolaceHeaderMapper implements SolaceHeaderMapper {

    private final SolaceHeaderMapper delegate = new DefaultSolaceHeaderMapper();
    private final Tracer tracer;

    @Override
    public void fromHeaders(Map<String,Object> headers, XMLMessage message) {
        Map<String,Object> enriched = new HashMap<>(headers);
        Span span = tracer.currentSpan();
        if (span != null) {
            enriched.put("traceparent", span.context().traceId());
        }
        delegate.fromHeaders(enriched, message);
    }

    @Override
    public Map<String,Object> toHeaders(BytesXMLMessage message) {
        return delegate.toHeaders(message);
    }
}
```

Delegating rather than reimplementing keeps the field mapping and the never-written list correct.

## 14.4 An error handler

```java
@Bean
DefaultSolaceListenerContainerFactory solaceListenerContainerFactory(
        SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
        SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds,
        SolaceProperties properties,
        @Qualifier("solaceTemplate") SolaceTemplate<Object> template,
        SolaceTransactionManager transactionManager,
        DeadLetterService deadLetters, MeterRegistry meters) {

    DefaultSolaceListenerContainerFactory factory = new DefaultSolaceListenerContainerFactory(
            sessionFactory, converter, headerMapper, instanceIds, properties.getListener());
    factory.setReplyTemplate(template);
    factory.setTransactionManager(transactionManager);
    factory.setErrorHandler((message, ex) -> {
        meters.counter("solace.listener.errors", "listener", "…").increment();
        deadLetters.record(message, ex);
    });
    return factory;
}
```

Note the `@Qualifier("solaceTemplate")` on the reply template. Without it the injection is ambiguous
once request-reply is on, and picking the `ReplyingSolaceTemplate` would route listener replies
through the template that tracks outstanding requests.

Remember the ordering the default handler is embedded in: on a **non-transactional** flow
`handleError` runs *before* the message is settled, so the message is still in hand; on a
**transactional** flow the rollback has already happened and the handler is purely for observation.

To decide each message's fate rather than merely report it, implement the interface's second method:

```java
factory.setErrorHandler(new SolaceListenerErrorHandler() {

    @Override
    public void handleError(BytesXMLMessage message, Exception exception) {
        deadLetters.record(message, exception);
    }

    @Override
    public SettlementOutcome resolveOutcome(BytesXMLMessage message, Exception exception) {
        if (exception instanceof SolaceMessagingException) {
            return SettlementOutcome.REJECTED;   // will never deserialise; skip the retries
        }
        if (DefaultSolaceHeaderMapper.deliveryCountOf(message) >= 3) {
            return SettlementOutcome.REJECTED;   // three strikes
        }
        return SettlementOutcome.FAILED;         // hand it back
    }
});
```

`resolveOutcome` is a `default` returning `null` — "the container decides" — so an error handler
written as a lambda keeps working unchanged. Set
**`solace.listener.negative-acknowledgement: true`** when using it: the container derives bind-time
negotiation from the configured `error-outcome`, and cannot know what a handler will return.

## 14.5 A second container factory

Better than replacing the default when only some listeners need different behaviour:

```java
@Bean
DefaultSolaceListenerContainerFactory batchListenerContainerFactory(
        SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
        SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds) {

    ContainerProperties properties = new ContainerProperties();
    properties.setDispatch(ContainerProperties.DispatchMode.EXECUTOR);
    properties.setDispatchQueueCapacity(2048);
    properties.setConcurrency(20);

    return new DefaultSolaceListenerContainerFactory(sessionFactory, converter, headerMapper,
            instanceIds, properties);
}
```

```java
@SolaceListener(queue = "bulk", topics = "bulk/>", containerFactory = "batchListenerContainerFactory")
public void onBulk(BulkEvent event) { … }
```

## 14.6 A bounded task executor

`SimpleAsyncTaskExecutor` creates a thread per task and does not pool. It is adequate because one
invoker per flow is submitted once and runs for the container's lifetime — but a real pool gives you
naming, metrics and MDC propagation:

```java
@Bean
AsyncTaskExecutor solaceListenerTaskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(16);
    executor.setMaxPoolSize(16);
    executor.setQueueCapacity(0);              // invokers must never queue: each runs forever
    executor.setThreadNamePrefix("solace-");
    executor.setTaskDecorator(new MdcTaskDecorator());
    executor.initialize();
    return executor;
}
```

Size it for the **total number of flows using EXECUTOR dispatch** across all containers. An invoker
occupies its thread for the container's whole lifetime, so an undersized pool means some flows never
start consuming.

## 14.7 An extra reply destination

```java
@Bean
ReplyingSolaceTemplate auditReplyingSolaceTemplate(ReplyingSolaceTemplateFactory factory) {
    ReplyEndpointSpec spec = new ReplyEndpointSpec();
    spec.setId("auditReplyContainer");
    spec.setReplyTopicPrefix("reply/audit");
    spec.setReplyTimeout(Duration.ofSeconds(5));
    return factory.create(spec);
}
```

`ReplyingSolaceTemplateFactory.createReplyContainer` is `protected`, so subclassing the factory lets
you change how the reply container is built while keeping the template wiring. See
[10.6](10-request-reply.md#106-when-to-split-a-reply-destination) for when this is warranted.

## 14.8 Custom instrumentation

Both metrics SPIs are public, carry no metrics-library types, and give every method a no-op default —
so implement only what you care about. Declaring either bean replaces the Micrometer implementation
entirely.

```java
@Bean
SolaceRequestReplyMetrics solaceRequestReplyMetrics(SlaRecorder sla) {
    return new SolaceRequestReplyMetrics() {
        @Override
        public void recordReply(String templateId, String destination, long latencyMillis) {
            sla.observe(destination, latencyMillis);
        }

        @Override
        public void recordTimeout(String templateId, String destination) {
            sla.breach(destination);
        }
    };
}
```

Both run on the message path, so they must be cheap. The call sites are guarded — an exception is
logged at debug and swallowed rather than failing the message — but an implementation that throws on
every message will fill the log.

To keep the Micrometer meters *and* add your own behaviour, delegate:

```java
@Bean
SolaceListenerMetrics solaceListenerMetrics(MeterRegistry registry, Tracer tracer) {
    SolaceListenerMetrics delegate = new MicrometerSolaceListenerMetrics(registry);
    return new SolaceListenerMetrics() {
        @Override public void recordReceived(String id) { delegate.recordReceived(id); }
        @Override public void recordSuccess(String id, long nanos) { delegate.recordSuccess(id, nanos); }
        @Override public void recordFailure(String id, long nanos, Exception ex) {
            delegate.recordFailure(id, nanos, ex);
            tracer.currentSpan().error(ex);
        }
    };
}
```

## 14.9 A custom health indicator

`solaceHealthIndicator` is `@ConditionalOnMissingBean(name = "solaceHealthIndicator")`, so a bean of
that name replaces it. Before writing one, check whether
`solace.health.require-all-containers-running: false` already expresses what you need — that is the
usual reason to want a different one.

`SolaceSessionFactory.isHealthy()` is a `default` method returning `true`, so a custom session factory
compiles unchanged and is simply reported as healthy. Override it if your implementation can cheaply
tell that its connection is gone.

## 14.10 A custom session factory

The heaviest extension point, and rarely needed. Implement `SolaceSessionFactory` if you need
per-tenant connections or pooling. Two rules the default implementation encodes and yours must too:

1. **Cache producers per session.** JCSMP refuses additional publisher flows on a session until that
   session's default publisher exists, so `createTransactedSession(session)` must ensure the
   producer first.
2. **Track and close every session you create.** Otherwise a restart leaks connections against the
   broker's client limit.

---

## 14.11 What is not extensible today

| | Why | Tracked in |
| :--- | :--- | :--- |
| Batch listeners | The container delivers one message per invocation | [18. Feature backlog](18-feature-backlog.md) |
| A retry/back-off policy inside the container | Redelivery is the broker's, via `max-redelivery-count` | [18](18-feature-backlog.md) |
| Pluggable argument resolvers on listener methods | The `MessageHandlerMethodFactory` is created internally | [18](18-feature-backlog.md) |
| Broker-side statistics as meters | `JCSMPSession` exposes session stats that are not sampled | [18](18-feature-backlog.md) |
| Broker administration beyond provisioning | Out of scope; use SEMP | — |

Take a `SolaceRecord<T>` or a `BytesXMLMessage` parameter as the escape hatch for the third of these:
anything the argument resolvers do not surface is reachable from the raw message.

---

**Next:** [15. Class reference](15-class-reference.md)
