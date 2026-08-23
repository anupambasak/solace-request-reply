# 12. Conversion and headers

Two small interfaces separate "what a message means" from "what a message is". Everything about
payload format and header representation goes through them, and both are replaceable.

---

## 12.1 `SolaceMessageConverter`

```java
public interface SolaceMessageConverter {
    XMLMessage toMessage(Object payload);
    Object fromMessage(BytesXMLMessage message, Class<?> targetType);
}
```

The counterpart of Spring for Kafka's `RecordMessageConverter`. Deliberately minimal: body in, body
out, no header involvement (that is the header mapper's job) and no destination involvement.

### `JacksonSolaceMessageConverter` — the default

- **Outbound:** serialises the payload to JSON bytes and writes them with `BytesMessage.setData()`.
  A `byte[]` payload is written as-is; a `String` is written as UTF-8.
- **Inbound:** reads the body and deserialises into `targetType`. `byte[]` and `String` targets skip
  Jackson entirely; `Object` deserialises to a `Map`.

It uses the application's `ObjectMapper` when the context has one — the auto-configuration injects it
through an `ObjectProvider` — so your Jackson modules, naming strategy and date handling apply
automatically. Otherwise it creates a default mapper.

### Where the body lives

```java
private byte[] extractBody(BytesXMLMessage message) {
    if (message instanceof TextMessage textMessage) { … }        // 1. TextMessage
    ByteBuffer attachment = message.getAttachmentByteBuffer();    // 2. binary attachment
    if (attachment != null && attachment.hasRemaining()) { … }
    byte[] xmlContent = message.getBytes();                       // 3. XML content part
    return xmlContent == null ? new byte[0] : xmlContent;
}
```

**This ordering matters.** A JCSMP `BytesMessage` carries its body in the **binary attachment**:
`setData()` writes there. `getBytes()` reads the **XML content part** — a different section of the
message, which comes back empty for a message written with `setData()`. Reading the wrong one
produces `MismatchedInputException: No content to map` with a message that plainly had content
(`attLen=43, contentLen=0` in a JCSMP dump).

The XML content part is kept as a fallback so messages from producers that use it still work. An
empty body raises an explicit `SolaceMessagingException` rather than a confusing Jackson error.

### Writing your own

```java
public class ProtobufSolaceMessageConverter implements SolaceMessageConverter {

    @Override
    public XMLMessage toMessage(Object payload) {
        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        message.setData(((MessageLite) payload).toByteArray());
        return message;
    }

    @Override
    public Object fromMessage(BytesXMLMessage message, Class<?> targetType) {
        ByteBuffer attachment = message.getAttachmentByteBuffer();
        byte[] bytes = new byte[attachment.remaining()];
        attachment.get(bytes);
        return parser(targetType).parseFrom(bytes);
    }
}
```

```java
@Bean
SolaceMessageConverter solaceMessageConverter() {
    return new ProtobufSolaceMessageConverter();
}
```

That single bean replaces conversion for the template, every listener, and request-reply.

---

## 12.2 `SolaceHeaderMapper`

```java
public interface SolaceHeaderMapper {
    void fromHeaders(Map<String,Object> headers, XMLMessage message);   // outbound
    Map<String,Object> toHeaders(BytesXMLMessage message);              // inbound
}
```

The counterpart of `KafkaHeaderMapper`. It translates between Spring's flat `Map<String,Object>` and
Solace's split model of *typed message fields* plus an *SDT user property map*.

### `SolaceHeaders` — the well-known names

Prefix `solace_` marks a Solace message field rather than a user property.

| Constant | Value | Direction | Maps to |
| :--- | :--- | :--- | :--- |
| `CORRELATION_ID` | `solace_correlationId` | both | `setCorrelationId` / `getCorrelationId` |
| `REPLY_TO` | `solace_replyTo` | both | `setReplyTo` / `getReplyTo().getName()` |
| `DESTINATION` | `solace_destination` | inbound only | `getDestination().getName()` |
| `APPLICATION_MESSAGE_ID` | `solace_applicationMessageId` | both | `setApplicationMessageId` |
| `SENDER_TIMESTAMP` | `solace_senderTimestamp` | inbound only | `getSenderTimestamp()` |
| `REDELIVERED` | `solace_redelivered` | inbound only | `getRedelivered()` |
| `DELIVERY_COUNT` | `solace_deliveryCount` | inbound only | `getDeliveryCount()`, or `-1` when unsupported |
| `TIME_TO_LIVE` | `solace_timeToLive` | outbound only | `setTimeToLive` — per message |
| `PRIORITY` | `solace_priority` | outbound only | `setPriority` |
| `TARGET_DESTINATION` | `solace_targetDestination` | routing only | Never written to the message |
| `RAW_MESSAGE` | `solace_rawMessage` | inbound only | The `BytesXMLMessage` itself |
| `INSTANCE_ID` | `instanceId` | both | A **user property** — no prefix |
| `REQUEST_SEND_TIME` | `requestSendTime` | both | A **user property** — no prefix |

`instanceId` and `requestSendTime` deliberately have no `solace_` prefix: they are request-reply
metadata carried as ordinary SDT user properties, so they survive a round trip through any
responder, including one not written with this library.

### Outbound rules

1. Recognised `solace_*` names set the corresponding message field.
2. Everything else becomes an SDT user property, so `@Header("tenant")` works on the other side.
3. These are **never** written: `solace_rawMessage`, `solace_destination`, `solace_redelivered`,
   `solace_deliveryCount`, `solace_targetDestination`, and Spring's own `id` and `timestamp` — they
   are either inbound-only metadata or routing instructions.
4. `solace_replyTo` resolves through `DefaultSolaceHeaderMapper.toDestination`: a value prefixed
   `queue:` becomes a queue, anything else a topic.

### Inbound rules

Every populated message field becomes a `solace_*` entry, then every SDT user property is copied in
under its own name. A property that cannot be read is logged at debug and skipped rather than failing
the whole message.

### `sanitize`

`DefaultSolaceHeaderMapper.sanitize(Map)` strips the never-written names from a header map. The
listener adapters use it when a listener returns a `Message<?>`, so inbound metadata does not leak
onto the reply.

---

## 12.3 `SolaceRecord<T>`

The counterpart of `ConsumerRecord`: the converted payload plus everything about the message that is
not the payload.

| Accessor | Meaning |
| :--- | :--- |
| `getPayload()` | The converted body |
| `getDestination()` | The topic or queue it arrived on |
| `getCorrelationId()` | For request-reply matching |
| `getReplyTo()` | Where a reply should go, if the sender asked for one |
| `getHeaders()` | Every mapped header, including `solace_rawMessage` |
| `getRawMessage()` | The `BytesXMLMessage`, for anything not surfaced |
| `isRedelivered()` | **True on a retry** — the hook for idempotency |
| `getDeliveryCount()` | How many times the broker has delivered it: `1` first time, `-1` when unsupported |
| `isDeliveryCountSupported()` | Whether the count is real. **Check this before comparing the number** — `-1` reads as a first delivery |

```java
@SolaceListener(pattern = "POINT_TO_POINT", queue = "orders", group = "workers")
public void onOrder(SolaceRecord<Order> record) {
    if (record.isRedelivered() && alreadyProcessed(record.getCorrelationId())) {
        return;                       // idempotent replay
    }
    if (record.isDeliveryCountSupported() && record.getDeliveryCount() > 3) {
        log.warn("Order {} is on attempt {}", record.getCorrelationId(), record.getDeliveryCount());
    }
    process(record.getPayload());
}
```

`isRedelivered()` is the always-available boolean; the delivery count is the number, and it is a
broker feature negotiated per message. Use the boolean for "have I seen this before", the count when
the policy depends on *how many times* — see [9.7](09-consuming-messages.md#97-delivery-count).

---

## 12.4 Choosing how to receive

| Signature | Use when |
| :--- | :--- |
| `void onX(Order order)` | You need only the payload. The common case. |
| `void onX(Order order, @Header("tenant") String tenant)` | You need one or two user properties. |
| `void onX(Message<Order> message)` | You want Spring's `Message` abstraction, e.g. to pass on to Spring Integration. |
| `void onX(SolaceRecord<Order> record)` | You need `isRedelivered()`, the delivery count, the destination, or the raw message. |
| `void onX(BytesXMLMessage message)` | You are doing something JCSMP-specific and want no conversion. |

The payload type is derived from the first non-framework parameter, so a listener taking only headers
and the raw message performs no body conversion at all.

---

**Next:** [13. Multi-instance and destinations](13-multi-instance.md)
