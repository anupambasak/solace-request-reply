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
| `SolaceListenerErrorHandler` | logging lambda | Route failures to a dead-letter service, metrics, alerting |
| `SolaceListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | Change how containers are built |
| `SolaceMessageListenerContainer` | `DefaultSolaceMessageListenerContainer` | Change consumption entirely |
| `SolaceMessageListener` | the adapters | Consume raw JCSMP messages |
| `AsyncTaskExecutor` (`solaceListenerTaskExecutor`) | `SimpleAsyncTaskExecutor` | Bound the pool, name threads, propagate MDC |

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

Remember the ordering rules the default handler is embedded in: on a **non-transactional** flow the
error handler runs and then `ack-on-error` decides whether to acknowledge; on a **transactional**
flow the rollback has already happened and the handler is purely for observation.

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

## 14.8 A custom session factory

The heaviest extension point, and rarely needed. Implement `SolaceSessionFactory` if you need
per-tenant connections or pooling. Two rules the default implementation encodes and yours must too:

1. **Cache producers per session.** JCSMP refuses additional publisher flows on a session until that
   session's default publisher exists, so `createTransactedSession(session)` must ensure the
   producer first.
2. **Track and close every session you create.** Otherwise a restart leaks connections against the
   broker's client limit.

---

## 14.9 What is not extensible today

| | Why | Tracked in |
| :--- | :--- | :--- |
| Batch listeners | The container delivers one message per invocation | [18. Feature backlog](18-feature-backlog.md) |
| A retry/back-off policy inside the container | Redelivery is the broker's, via `max-redelivery-count` | [18](18-feature-backlog.md) |
| Pluggable argument resolvers on listener methods | The `MessageHandlerMethodFactory` is created internally | [18](18-feature-backlog.md) |
| Broker administration beyond provisioning | Out of scope; use SEMP | — |

Take a `SolaceRecord<T>` or a `BytesXMLMessage` parameter as the escape hatch for the third of these:
anything the argument resolvers do not surface is reachable from the raw message.

---

**Next:** [15. Class reference](15-class-reference.md)
