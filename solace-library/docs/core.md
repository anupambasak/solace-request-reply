# `core` — sessions, templates, conversion

Package `cris.prs.messaging.solace.core`. Nothing here knows about listeners or request-reply.

---

## SolaceSessionFactory

Interface. Supplies connected `JCSMPSession`s — the analogue of Kafka's `ProducerFactory` and
`ConsumerFactory` combined. It is also the **resource key** transactions bind under, so every
component must share one instance.

| Method | Returns | Description |
| :--- | :--- | :--- |
| `getSharedSession()` | `JCSMPSession` | The shared, lazily connected session. JCSMP sessions are thread safe, so the template and all non-transactional flows use this one. |
| `createSession()` | `JCSMPSession` | A new connected session owned by the caller. Used by `DIRECT` containers and by transactional containers for their own transacted-session budget. |
| `getSharedProducer()` | `XMLMessageProducer` | The default publisher of the shared session. |
| `getProducer(JCSMPSession)` | `XMLMessageProducer` | The default publisher of a given session, created on first use. JCSMP requires a session's default publisher to exist before any other publisher flow on it. |
| `createTransactedSession()` | `TransactedSession` | A transacted session on the shared session. |
| `createTransactedSession(JCSMPSession)` | `TransactedSession` | A transacted session on a specific connection, so several transactional listeners can spread across connections. |
| `closeSession(JCSMPSession)` | `void` | Closes a session handed out by `createSession()`. Ignores the shared session. |

## DefaultSolaceSessionFactory

Implementation over the `SpringJCSMPFactory` contributed by `solace-java-spring-boot-starter`
(configured through `solace.java.*`). Implements `DisposableBean`.

* Keeps one default publisher **per session** in a map, because the "default publisher first" rule
  is per connection, not per application.
* `createTransactedSession(session)` calls `getProducer(session)` first, so a service that only ever
  publishes inside transactions still satisfies that rule.
* `destroy()` closes every producer and every session it created.

`LoggingPublishEventHandler` is a nested `JCSMPStreamingPublishCorrelatingEventHandler` that surfaces
asynchronous publish failures in the log — without it they are silent.

---

## SolaceOperations&lt;T&gt;

Publishing contract, the analogue of `KafkaOperations`. Every `send` joins the Solace transaction
bound to the current thread when one is active.

| Method | Description |
| :--- | :--- |
| `send(T payload)` | Publish to `defaultDestination`. Throws `IllegalStateException` when none is configured. |
| `send(String destination, T payload)` | Publish to a topic (or `queue:name` for a queue). |
| `send(String destination, String correlationId, T payload)` | As above, setting the native correlation id. |
| `send(String destination, T payload, Map<String,Object> headers)` | Extra headers become SDT user properties, usable in broker-side selectors. |
| `send(Message<?> message)` | Publish a Spring `Message`; the destination comes from the `solace_targetDestination` header. |
| `send(Destination destination, XMLMessage message)` | Publish an already-built Solace message. |
| `executeInTransaction(TransactionCallback<T,R>)` | Run the callback in a Solace local transaction, committing on return and rolling back on exception. Joins an existing transaction rather than nesting. |

## SolaceTemplate&lt;T&gt;

The Solace counterpart of `KafkaTemplate`: converts a payload, applies headers, publishes.

**Constructor:** `SolaceTemplate(SolaceSessionFactory, SolaceMessageConverter)` — both required.

| Property | Default | Description |
| :--- | :--- | :--- |
| `messageConverter` | constructor arg | Payload ↔ message body. |
| `headerMapper` | `DefaultSolaceHeaderMapper` | Headers ↔ native fields and SDT properties. |
| `defaultDestination` | `null` | Used by `send(payload)`. |
| `deliveryMode` | `PERSISTENT` | |
| `timeToLive` | `0` (never expire) | Milliseconds. |
| `priority` | `null` | |
| `dmqEligible` | `true` | Required for dead-message-queue routing. |

| Method | Description |
| :--- | :--- |
| `createMessage(Object payload, Map<String,Object> headers)` | Build a message with the template's defaults applied. Public so callers can pre-build and reuse. |
| `producer()` (protected) | The transacted producer when a transaction is active, otherwise the shared producer. Override to change producer selection. |
| `isTransactionActive()` | Whether a Solace transaction is bound to the calling thread. |

---

## SolaceMessageConverter

Interface: payload ↔ message body, the analogue of a Kafka `Serializer`/`Deserializer` pair.

| Method | Description |
| :--- | :--- |
| `toMessage(Object payload)` | Build a Solace message carrying the payload. Headers are applied separately by the header mapper. |
| `fromMessage(BytesXMLMessage message, Class<?> targetType)` | Convert a received body into the requested type. |

## JacksonSolaceMessageConverter

Default implementation: JSON in the **binary attachment** of a `BytesMessage`.

* `byte[]` and `String` payloads pass through untouched, so the converter also suits opaque or text
  protocols.
* Reading prefers the binary attachment (`getAttachmentByteBuffer()`) and falls back to the XML
  content part, because those are two different sections of a Solace message and `setData` writes
  the former.
* An empty body raises a `SolaceMessagingException` naming the problem rather than a Jackson
  end-of-input error.

**Constructors:** `JacksonSolaceMessageConverter()` (own `ObjectMapper`) and
`JacksonSolaceMessageConverter(ObjectMapper)` (share the application's).

---

## SolaceHeaderMapper

Interface: `fromHeaders(Map, XMLMessage)` and `toHeaders(BytesXMLMessage)`.

## DefaultSolaceHeaderMapper

Headers named after [`SolaceHeaders`](#solaceheaders) constants map to native Solace fields;
everything else becomes an SDT user property, so it stays usable in broker-side selectors.

| Method | Description |
| :--- | :--- |
| `fromHeaders(Map, XMLMessage)` | Apply headers to an outbound message. Framework-internal entries (`solace_rawMessage`, `solace_destination`, `solace_redelivered`, `solace_targetDestination`, `id`, `timestamp`) are skipped. |
| `toHeaders(BytesXMLMessage)` | Read native fields and all SDT properties into a header map. |
| `toDestination(Object)` *(static)* | Name → `Destination`. A `queue:` prefix yields a queue; anything else a topic. |
| `sanitize(Map)` *(static)* | Copy a header map without framework-internal entries. |

Supported SDT property types: `String`, `Integer`, `Long`, `Double`, `Boolean`, `byte[]`; anything
else is written as its `toString()`.

---

## SolaceHeaders

Constants for well-known headers. Those prefixed `solace_` map to native message fields; the rest are
SDT user properties.

| Constant | Value | Maps to |
| :--- | :--- | :--- |
| `CORRELATION_ID` | `solace_correlationId` | native correlation id |
| `REPLY_TO` | `solace_replyTo` | native reply-to, as a destination name |
| `DESTINATION` | `solace_destination` | destination received on (inbound only) |
| `APPLICATION_MESSAGE_ID` | `solace_applicationMessageId` | native application message id |
| `SENDER_TIMESTAMP` | `solace_senderTimestamp` | native sender timestamp |
| `REDELIVERED` | `solace_redelivered` | native redelivered flag (inbound only) |
| `TIME_TO_LIVE` | `solace_timeToLive` | native TTL, milliseconds |
| `PRIORITY` | `solace_priority` | native priority |
| `TARGET_DESTINATION` | `solace_targetDestination` | overrides the publish destination |
| `RAW_MESSAGE` | `solace_rawMessage` | the `BytesXMLMessage` itself, added inbound |
| `INSTANCE_ID` | `instanceId` | SDT property: which instance produced the request |
| `REQUEST_SEND_TIME` | `requestSendTime` | SDT property: publish time, for latency reporting |

---

## SolaceRecord&lt;T&gt;

A received message plus its converted payload — the analogue of `ConsumerRecord`.

Fields: `payload`, `destination`, `correlationId`, `replyTo`, `headers`, `rawMessage`.
`isRedelivered()` reports the broker's redelivery flag.

## EndpointMode

`DURABLE_QUEUE`, `NON_DURABLE_QUEUE`, `DIRECT`. `isQueueBased()` is false only for `DIRECT`, which
supports no acknowledgement or transactions.

## ExchangePattern

`PUBLISH_SUBSCRIBE`, `POINT_TO_POINT`, `REQUEST_REPLY`. See [Exchange patterns](exchange-patterns.md).

## SolaceMessagingException

Unchecked wrapper for the checked `JCSMPException` hierarchy, extending Spring's
`NestedRuntimeException`.
