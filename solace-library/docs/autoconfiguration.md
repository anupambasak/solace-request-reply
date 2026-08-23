# `autoconfigure` — Spring Boot integration

Package `cris.prs.solace.autoconfigure`.

> **This package deliberately sits outside `cris.prs.messaging.solace`.** An auto-configuration
> class that an application component-scans is treated as an ordinary `@Configuration`, so its
> conditions are evaluated before the Solace starter has contributed `SpringJCSMPFactory` — and every
> bean below is silently skipped. Keeping the package outside any plausible application base package
> makes that impossible. See [Troubleshooting](troubleshooting.md#no-bean-named-solacelistenercontainerfactory-available).

---

## SolaceAutoConfiguration

Registered in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`,
ordered after `com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration`, conditional on
`JCSMPSession` and `SpringJCSMPFactory` being on the classpath.

The Solace equivalent of Spring Boot's `KafkaAutoConfiguration` — and like that one it turns on
annotation-driven listeners automatically, so applications need not declare `@EnableSolace`.

| Bean | Type | Condition | Notes |
| :--- | :--- | :--- | :--- |
| `solaceInstanceIdProvider` | `InstanceIdProvider` | missing bean | `HostnameInstanceIdProvider` seeded with `solace.instance-id`. |
| `solaceMessageConverter` | `SolaceMessageConverter` | missing bean | `JacksonSolaceMessageConverter`, reusing the application `ObjectMapper` when there is one. |
| `solaceHeaderMapper` | `SolaceHeaderMapper` | missing bean | `DefaultSolaceHeaderMapper`. |
| `solaceSessionFactory` | `SolaceSessionFactory` | missing bean | `DefaultSolaceSessionFactory` over `SpringJCSMPFactory`. |
| `solaceTransactionManager` | `SolaceTransactionManager` | missing bean | Enables `@Transactional` and `TransactionTemplate`. |
| `solaceTemplate` | `SolaceTemplate<Object>` | missing bean by name | **`@Primary`.** |
| `solaceListenerTaskExecutor` | `AsyncTaskExecutor` | missing bean by name | `SimpleAsyncTaskExecutor("solace-listener-")`, non-daemon. |
| `solaceListenerContainerFactory` | `DefaultSolaceListenerContainerFactory` | missing bean by name | Wired with the transaction manager, the reply template and the task executor. |
| `replyingSolaceTemplateFactory` | `ReplyingSolaceTemplateFactory` | missing bean | Builds templates and their reply containers; declare more beans from it for additional reply destinations. |
| `replyingSolaceTemplate` | `ReplyingSolaceTemplate` | `solace.request-reply.enabled` ≠ false | The application's default template, built from `solace.request-reply.*`. |

### Why `solaceTemplate` is `@Primary`

`ReplyingSolaceTemplate extends SolaceTemplate`, so both beans satisfy an unqualified
`SolaceTemplate` injection point once request-reply is enabled. Application code asking for a plain
template gets `solaceTemplate`; asking for request-reply means injecting `ReplyingSolaceTemplate` by
its own type. The container factory qualifies it explicitly as well, so listener replies never go
through the template that is also tracking outstanding requests.

### The reply container

Built by `ReplyingSolaceTemplateFactory` rather than by the listener container factory, because its
listener is supplied by the template. Its endpoint carries the instance id in both the topic
subscription and — for queue-based modes — the endpoint name. It is configured with
`autoStartup = false` and `keepAlive = false`: the requesting application decides its own lifetime,
and `ReplyingSolaceTemplate.start()` starts the container so no reply can arrive before the
correlation map exists. It is not a bean; the template owns it.

`SolaceProperties.RequestReply` extends `ReplyEndpointSpec`, so `solace.request-reply.*` and a
hand-declared spec describe a reply destination the same way — see
[Additional reply destinations](request-reply.md#additional-reply-destinations).

## SolaceAnnotationDrivenConfiguration

`@Configuration` carrying `@EnableSolace`, conditional on there being no
`SolaceListenerAnnotationBeanPostProcessor` — mirroring Spring Boot's
`KafkaAnnotationDrivenConfiguration`. An application that declares `@EnableSolace` itself wins.

## SolaceBootstrapConfiguration

`ImportBeanDefinitionRegistrar` imported by `@EnableSolace`. Registers, as
`ROLE_INFRASTRUCTURE` beans under the well-known names in `SolaceListenerConfigUtils`:

* `SolaceListenerAnnotationBeanPostProcessor`
* `SolaceListenerEndpointRegistry`

Both registrations are guarded by a `containsBeanDefinition` check, so importing `@EnableSolace`
more than once is harmless.

## SolaceProperties

`@ConfigurationProperties(prefix = "solace")`. See the
[configuration reference](configuration.md) for every property and default.

| Nested type | Prefix | Purpose |
| :--- | :--- | :--- |
| `Template` | `solace.template` | Defaults for the auto-configured templates. |
| `Listener` (extends `ContainerProperties`) | `solace.listener` | Defaults for every listener container. |
| `RequestReply` | `solace.request-reply` | The reply destination and timeout. |

`Listener` extends `ContainerProperties` rather than duplicating it, so a property added to the
container is bindable without a second declaration.

## Overriding

Every bean is `@ConditionalOnMissingBean`, so declaring your own replaces it.

Two of them — `solaceTemplate` and `replyingSolaceTemplate` — match **by bean name** rather than by
type, because an application is expected to declare *additional* beans of those types: a second
`SolaceTemplate` with different defaults, or a second `ReplyingSolaceTemplate` giving one service its
own reply destination. An extra bean of either type is added alongside the auto-configured one; only
a bean with the same *name* replaces it. Every other bean here matches by type.

```java
@Bean
SolaceMessageConverter solaceMessageConverter() {
    return new ProtobufSolaceMessageConverter();
}

@Bean
DefaultSolaceListenerContainerFactory batchListenerContainerFactory(
        SolaceSessionFactory sessionFactory, SolaceMessageConverter converter,
        SolaceHeaderMapper headerMapper, InstanceIdProvider instanceIds) {
    ContainerProperties properties = new ContainerProperties();
    properties.setConcurrency(20);
    return new DefaultSolaceListenerContainerFactory(sessionFactory, converter, headerMapper,
            instanceIds, properties);
}
```

```java
@SolaceListener(containerFactory = "batchListenerContainerFactory", topics = "bulk/in")
public void onBulk(Payload payload) { }
```

Disable the whole library with:

```yaml
spring:
  autoconfigure:
    exclude: cris.prs.solace.autoconfigure.SolaceAutoConfiguration
```
