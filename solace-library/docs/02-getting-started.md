# 2. Getting started

Everything on this page is complete and runnable. It assumes a reachable Solace PubSub+ broker.

---

## 2.1 Add the dependency

Gradle, with the Solace Spring Boot BOM applied so JCSMP versions line up:

```groovy
dependencyManagement {
    imports {
        mavenBom org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES
        mavenBom "com.solace.spring.boot:solace-spring-boot-bom:2.5.0"
    }
}

dependencies {
    implementation project(":solace-library")     // or the published coordinates
}
```

The library brings its own transitive dependencies:

| Dependency | Why |
| :--- | :--- |
| `com.solace.spring.boot:solace-java-spring-boot-starter` | Supplies the `SpringJCSMPFactory` bean this library builds on, and binds `solace.java.*` |
| `org.springframework:spring-messaging` | `Message`, `MessageHeaders`, `InvocableHandlerMethod` |
| `org.springframework:spring-tx` | `PlatformTransactionManager` and friends |
| `com.fasterxml.jackson.core:jackson-databind` | Default payload conversion |
| `org.springframework.boot:spring-boot-starter-actuator` | Health/metrics surface for the host app |

Schema registry support is optional. It is backed by Apicurio Registry: add
`io.apicurio:apicurio-registry-serde-common-avro`, `…-protobuf` and/or `…-jsonschema` only for the formats
you use. See [19. Schema Registry](19-schema-registry.md).

## 2.2 Configure the broker connection

The connection itself is *not* this library's concern — it belongs to the Solace starter:

```yaml
solace:
  java:
    host: tcp://localhost:55555
    msg-vpn: default
    client-username: default
    client-password: default
```

That alone is enough. Everything under `solace.template.*`, `solace.listener.*` and
`solace.request-reply.*` is optional and has working defaults — see
[5. Configuration reference](05-configuration.md).

## 2.3 There is no `@EnableSolace` step

Auto-configuration turns annotation-driven listeners on for you, exactly as Spring Boot does for
Kafka. A plain `@SpringBootApplication` is all that is required. `@EnableSolace` exists for
non-Boot Spring applications and for tests that build a context by hand —
see [4. Spring integration](04-spring-integration.md#46-enablesolace-and-the-bootstrap-registrar).

---

## 2.4 Send a message

```java
@Service
public class OrderPublisher {

    private final SolaceTemplate<Object> solace;

    public OrderPublisher(SolaceTemplate<Object> solace) {   // the auto-configured @Primary bean
        this.solace = solace;
    }

    public void publish(Order order) {
        solace.send("orders/created", order);
    }
}
```

`send` converts the payload with the configured `SolaceMessageConverter`, applies the template's
delivery mode, TTL, priority and DMQ-eligibility defaults, and publishes. It is synchronous with
respect to handing the message to JCSMP; with `PERSISTENT` delivery the broker acknowledgement is
handled asynchronously and logged. See [8. Producing messages](08-producing-messages.md).

## 2.5 Receive messages

```java
@Component
public class OrderListener {

    @SolaceListener(
            pattern = "POINT_TO_POINT",
            queue = "orders",
            group = "workers",
            topics = "orders/created",
            concurrency = "5")
    public void onOrder(Order order) {
        // one instance in the group gets each order
    }
}
```

That declaration provisions the durable queue `orders.workers`, subscribes it to `orders/created`,
and binds five flows to it. Every instance of the application binds to the *same* queue, so the
messages are shared out — competing consumers. See [7. Exchange patterns](07-exchange-patterns.md).

For broadcast instead, one line changes:

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "audit", topics = "orders/created")
public void onOrderForAudit(Order order) {
    // every instance receives every message
}
```

## 2.6 Request and reply

**The responder** returns a value. That is the whole opt-in — no output binding, no reply topic:

```java
@Component
public class PricingService {

    @SolaceListener(pattern = "REQUEST_REPLY", queue = "pricing", group = "v1",
            topics = "pricing/quote", transactional = "true")
    public Quote quote(PriceRequest request) {
        return new Quote(request.sku(), price(request));
    }
}
```

**The requester** uses `ReplyingSolaceTemplate`:

```java
@Service
public class PricingClient {

    private final ReplyingSolaceTemplate solace;

    public PricingClient(ReplyingSolaceTemplate solace) {
        this.solace = solace;
    }

    public Mono<Quote> quote(PriceRequest request) {
        RequestReplyFuture<Quote> future =
                solace.sendAndReceive("pricing/quote", request, Quote.class);
        return Mono.fromFuture(future);
    }
}
```

The requester stamps each request with a correlation id and its own reply destination
(`reply/<pod-name>` by default), consumes that destination, and completes the matching future when
the reply arrives. The responder never chooses where to reply — it echoes the request's `replyTo`.
See [10. Request-reply](10-request-reply.md).

## 2.7 Make it transactional

The consume and the publish inside a listener commit together when the flow is transacted:

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers",
        topics = "orders/created", transactional = "true")
public void onOrder(Order order) {
    solace.send("orders/audited", audit(order));   // same transaction as the acknowledgement
}                                                  // throw here and both roll back
```

On the sending side, use `@Transactional` or a `TransactionTemplate`:

```java
@Transactional("solaceTransactionManager")
public void publishBatch(List<Order> orders) {
    orders.forEach(order -> solace.send("orders/created", order));
}   // nothing reaches the broker until this returns normally
```

See [11. Transactions](11-transactions.md).

---

## 2.8 What you now have running

At this point the context contains, without any further configuration:

```
solaceInstanceIdProvider      → HostnameInstanceIdProvider
solaceMessageConverter        → JacksonSolaceMessageConverter
solaceHeaderMapper            → DefaultSolaceHeaderMapper
solaceSessionFactory          → DefaultSolaceSessionFactory
solaceTransactionManager      → SolaceTransactionManager
solaceTemplate                → SolaceTemplate<Object>            (@Primary)
solaceListenerTaskExecutor    → SimpleAsyncTaskExecutor
solaceListenerContainerFactory→ DefaultSolaceListenerContainerFactory
replyingSolaceTemplateFactory → ReplyingSolaceTemplateFactory
replyingSolaceTemplate        → ReplyingSolaceTemplate
+ the annotation post-processor and the endpoint registry
```

Every one of them is replaceable. See [4. Spring integration](04-spring-integration.md) and
[14. Extension points](14-extension-points.md).

---

**Next:** [3. Architecture](03-architecture.md)
