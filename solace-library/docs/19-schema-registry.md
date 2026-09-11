# 19. Schema Registry

Schema-governed payloads in **Apache Avro**, **Google Protocol Buffers** or **JSON Schema**, validated
against and versioned in [Apicurio Registry](https://www.apicur.io/registry/), through the same
`SolaceTemplate`, `ReplyingSolaceTemplate` and `@SolaceListener` code as plain JSON. Turning it on is
configuration plus the Apicurio module for each format you use. Listener signatures don't change, and
neither do payload classes.

The Spring for Apache Kafka analogue is swapping `JsonSerializer` for Apicurio's `AvroKafkaSerializer`.
This library uses Apicurio's **generic** serde modules, the ones with no Kafka dependency, beneath its own
converter.

---

## 19.1 Turning it on

Add the Apicurio serde module for each format you use:

```groovy
dependencies {
    implementation 'io.apicurio:apicurio-registry-serde-common-avro:3.3.3'        // Avro
    implementation 'io.apicurio:apicurio-registry-serde-common-protobuf:3.3.3'    // Protobuf
    implementation 'io.apicurio:apicurio-registry-serde-common-jsonschema:3.3.3'  // JSON Schema
}
```

Then point the library at the registry:

```yaml
solace:
  schema-registry:
    url: http://apicurio-registry:8080/apis/registry/v3
    # formats: [AVRO, PROTOBUF, JSON_SCHEMA]  # default: every format whose module is on the classpath
    username: ${REGISTRY_USER}
    password: ${REGISTRY_PASSWORD}
    destinations:                           # topics where POJOs are governed (JSON Schema); empty = all
      - orders/>
    topic-profile:                          # which artifact each topic uses
      - topic-expression: "orders/place"
        artifact-id: order-request
      - topic-expression: "app/reply/>"     # every instance's reply topic, one artifact
        artifact-id: order-reply
  listener:
    negative-acknowledgement: true          # lets poison messages be REJECTED; see 19.7
```

That's all. `solaceMessageConverter` becomes a `SchemaRegistrySolaceMessageConverter`. Every template,
listener container, request-reply template and queue browser already uses that bean.

**Fail-fast.** Startup fails with a message naming the fix in three cases: `url` is set but no Apicurio
serde module is on the classpath, a format listed in `formats` has no module, or the settings are
incomplete. There's deliberately no silent fallback to plain JSON. An application that configured a
registry expects its messages governed.

---

## 19.2 The three formats

| Format | Payload you send | Types a listener can ask for | Apicurio module |
| :--- | :--- | :--- | :--- |
| `AVRO` | any Avro record: `GenericRecord` or a generated `SpecificRecord` | `GenericRecord`, or the generated class | `apicurio-registry-serde-common-avro` |
| `PROTOBUF` | any `com.google.protobuf.Message`: a generated class or `DynamicMessage` | the generated class, `DynamicMessage`, `Message` | `apicurio-registry-serde-common-protobuf` |
| `JSON_SCHEMA` | a POJO on a governed destination, or a Jackson `JsonNode` anywhere | any POJO, or `JsonNode` | `apicurio-registry-serde-common-jsonschema` |

All three can be enabled at once. Outbound, the payload's type picks the format. Inbound, the message
says which format it is (19.4).

**Avro.** Two deserializers are created on demand: one with Apicurio's specific reader, for listeners
that ask for a generated `SpecificRecord`, and one for everything else, which yields `GenericRecord`.

**Protobuf.** Apicurio writes the message type name ahead of the body, so one schema can hold several
message types. One deserializer yields a `DynamicMessage`. When the listener asks for a generated class,
the converter re-parses that `DynamicMessage` with the class's static `parseFrom(ByteString)`. Apicurio's
own option, a fixed return class, allows only one class per deserializer, and one deserializer can't then
serve listeners that want different types. With `protobuf.derive-class: true` Apicurio returns the class
named by the schema's `java_outer_classname` and `java_multiple_files` directly.

**JSON Schema.** Serialised with **your** `ObjectMapper`, with its naming strategy, modules and date
handling. The JSON on the wire is therefore exactly what the plain converter would have produced, and
moving a destination under the registry adds validation without changing the payload. It deserialises to
a `JsonNode`, which the converter maps onto each listener's type with the same mapper. One validating
deserializer can therefore serve every listener, no schema needs a `javaType`, and payload classes need no
schema dependency. A schema that does carry `javaType` still works: when Apicurio returns the listener's
type, it's used as it is. JSON Schema can't be inferred from a POJO the way Avro and Protobuf schemas are
inferred from their payloads. To auto-register one, point `json-schema.schema-location` at a classpath
schema.

---

## 19.3 What goes through the registry

### Outbound

| Payload | Destination | Result |
| :--- | :--- | :--- |
| `null`, `byte[]`, `String` | any | Plain, as without a registry |
| An Avro record, a Protobuf message or a `JsonNode` | any | That format's codec |
| Any other object | matches `destinations` | JSON Schema. If JSON Schema isn't enabled, a `TYPE_MISMATCH` failure |
| Any other object | doesn't match | Plain JSON |

### Inbound

| Listener asks for | Message body | Result |
| :--- | :--- | :--- |
| `Object`, `BytesXMLMessage`, `byte[]`, `String` | any | Raw, no registry call |
| anything else | registry-framed | Registry; see 19.4 for how the format is chosen |
| anything else | not framed | Plain JSON. With `require-schema-id` on and a governed destination, `MISSING_SCHEMA_ID` instead |

A governed consumer can therefore keep working while producers migrate one at a time. Turn on
`require-schema-id` once they have all moved.

Everything that uses the converter inherits this behaviour:

- **Topic dispatch:** each target keeps its own type.
- **Request-reply:** replies are converted the same way.
- **`browse(...)`:** a DMQ of rejected messages can be inspected with the same types.

---

## 19.4 On the wire

The body uses Apicurio's standard framing, the same bytes Apicurio's Kafka serdes write:

```
[0x00][schema id: 4 bytes][encoded payload]
```

The id is the content id by default (`use-id: GLOBAL_ID` switches to the global id; producers and
consumers must agree). A message produced here is readable by any Apicurio consumer, and the reverse, so
a Kafka-to-Solace bridge carries governed payloads through unchanged.

**Detection** is by the magic byte. Plain JSON never starts with a `NUL` byte, so a registry-framed body
can't be confused with one from the fallback converter.

**The format** is also written as an SDT user property, `schemaFormat` (`AVRO`, `PROTOBUF` or `JSON`).
There are two reasons:

- A consumer with several formats enabled can pick the right deserializer without guessing.
- A broker-side selector can filter on it, for example `schemaFormat = 'AVRO'`.

A framed message without the property, such as one from a producer that doesn't use this library, gets
a codec chosen from what's known:

1. the listener's type, if it's specific to one format (an Avro record type, a Protobuf message type);
2. otherwise JSON Schema, if it's enabled;
3. otherwise the only enabled format;
4. otherwise an `UNSUPPORTED_FORMAT` failure.

A property naming a format that isn't enabled is also `UNSUPPORTED_FORMAT`, never a guess.

**Headers.** The `schemaFormat` property shows up among an inbound message's headers like any other user
property. A listener that replies with `MessageBuilder…copyHeaders(request.getHeaders())` would carry
the request's format onto a reply that may be in a different format. `DefaultSolaceHeaderMapper` never
overwrites a user property the converter has already written, so the reply keeps its own format. See
[12.2](12-conversion-and-headers.md#122-solaceheadermapper).

---

## 19.5 Where the schema comes from: artifact resolution

When serialising, Apicurio looks up the artifact the payload is validated against (or registered as)
through an *artifact resolver strategy*. Deserialising needs no strategy, because the id in the body
names the schema. `artifact-resolver-strategy` selects the strategy:

| Value | Strategy | Artifact id |
| :--- | :--- | :--- |
| `TOPIC_PROFILE` **(default)** | this library's `SolaceTopicProfileStrategy` | from the first `topic-profile` mapping whose Solace expression matches the topic |
| `DESTINATION` | Apicurio `SimpleTopicIdStrategy` | the topic name |
| `TOPIC` | Apicurio `TopicIdStrategy` | the topic name + `-value`, as Apicurio's Kafka serdes do |
| `RECORD` | Apicurio Avro `RecordIdStrategy` | the Avro record's full name. **Avro only** |
| a class name | any `ArtifactReferenceResolverStrategy` | yours |

`TOPIC_PROFILE` mappings are Solace topic expressions: `*` matches one level, and a trailing `>` matches
the rest. They are matched client-side with the broker's rules and tried in order; the first match wins.
Each mapping needs an `artifact-id`; `group-id` and `version` are optional. A topic that no mapping
matches is a `SCHEMA_NOT_FOUND` failure. Use `explicit-artifact.*` instead to pin every serialisation to
one artifact.

Without a `version`, Apicurio resolves the artifact by the payload's schema content (Avro, Protobuf), or
by `find-latest: true`. For JSON Schema, set `find-latest` or a version in the mapping.

`RECORD` is Avro-only. To use it for Avro alongside other formats, keep `TOPIC_PROFILE` and override Avro
alone:

```yaml
    avro:
      properties:
        "[apicurio.registry.artifact-resolver-strategy]": io.apicurio.registry.serde.avro.strategy.RecordIdStrategy
```

### Per-instance reply topics

Replies go to `<reply-topic-prefix>/<instance-id>` ([13](13-multi-instance.md)). Under `DESTINATION` or
`TOPIC`, every pod's reply topic is a different artifact id. With `auto-register` on, that means **one
artifact per pod, forever**; without it, every reply fails to serialise. The configuration logs a warning
for this combination.

**Map the reply prefix with `>`**, never the resolved reply topic:

```yaml
    topic-profile:
      - topic-expression: "app/reply/>"
        artifact-id: order-reply
```

### A shared reply destination carries several reply types

One reply destination per instance is shared by every service it calls
([10.5](10-request-reply.md#105-multi-instance-handling)). It can therefore carry replies of several types,
and no single topic mapping fits all of them. Four ways out:

1. **Avro:** `RECORD`. The schema follows the record, so one destination can carry any number of types.
2. **Different formats per service:** the `schemaFormat` property separates them on the way in, but
   outbound mapping is still per topic, so this only helps when each service's replies are in a
   different format.
3. **Its own reply destination:** give the governed service one with a `ReplyEndpointSpec`
   ([10.6](10-request-reply.md#106-when-to-split-a-reply-destination)). This is a fifth reason to split
   one out.
4. **Ungoverned replies:** leave the reply prefix out of `destinations` and send POJO replies, so replies
   travel as plain JSON while requests are governed.

---

## 19.6 Registry availability and caching

Nothing contacts the registry at startup. Apicurio serializers and deserializers are created on the first
message that needs them, so an instance starts even while the registry is down. A mistake in raw
`apicurio.registry.*` properties therefore surfaces on that first message.

Lookups are cached (`cache.check-period`, Apicurio default 30 s). The library changes two Apicurio
defaults:

| Property | Apicurio default | Library default | Why |
| :--- | :--- | :--- | :--- |
| `cache.fault-tolerant-refresh` | `false` | **`true`** | With `false`, a registry outage fails every message once the cache period lapses, while the broker and the application are both healthy. With `true` the cached schema keeps being used, and an outage means "no new schemas" rather than "no messages". |
| `http-adapter` | `AUTO` (Vert.x when present) | **`JDK`** | Every serializer and deserializer owns a registry client. With Vert.x, each client brings its own event loop, and Vert.x's Netty sits beside the one Spring WebFlux already uses. The JDK `HttpClient` needs neither, and supports the same authentication and TLS options. |

There is **no registry health indicator**. `SolaceHealthIndicator` never makes a network call, and a
readiness probe that failed on a registry blip would take every instance out of rotation at once. That's
exactly what the cache default exists to prevent.

The registry sits on two hot paths:

- **Inbound** conversion runs on the JCSMP delivery thread for `INLINE` dispatch, which covers every
  transactional container.
- **Outbound** conversion inside `@Transactional` holds the transacted session.

Once a schema is cached, both paths cost nothing extra: Apicurio's serdes keep a fast-path cache keyed by
schema id and class. Only the first message for each schema waits for a registry round trip.

The codecs are a Spring bean, closed on shutdown along with every Apicurio client. If you create
`SchemaCodecs` yourself, you must close it yourself.

---

## 19.7 Failures and settlement

Every failure is a `SchemaRegistryConversionException` carrying a `Reason`:

| Reason | Example | Retryable | `SchemaRegistryErrorHandler` |
| :--- | :--- | :--- | :--- |
| `REGISTRY_UNAVAILABLE` | connection refused, timeout, HTTP 5xx/429; also 401/403, a configuration fault | yes | defers to the container (`errorOutcome`) |
| `SCHEMA_NOT_FOUND` | HTTP 404; the id in the body is unknown; no `topic-profile` mapping matches | no | `REJECTED` |
| `VALIDATION_FAILED` | the payload doesn't match its schema, or its bytes don't decode | no | `REJECTED` |
| `MISSING_SCHEMA_ID` | strict mode, body not registry-framed | no | `REJECTED` |
| `UNSUPPORTED_FORMAT` | the message's format isn't enabled, or can't be determined | no | `REJECTED` |
| `TYPE_MISMATCH` | the decoded value can't become the listener's type; a POJO with JSON Schema off | no | `REJECTED` |
| `UNCLASSIFIED` | anything the serde throws that isn't recognised | yes | defers |

Classification is by the cause chain:

1. A reason already in the chain, such as one thrown by `SolaceTopicProfileStrategy` from inside
   Apicurio, is kept.
2. Otherwise the registry client's HTTP status is used. Apicurio's client is generated with Kiota, and
   the status is read reflectively from its `ApiException`.
3. Otherwise the exception's type and message are used.

Unrecognised failures are deliberately treated as *retryable*. Rejecting a good message to the DMQ is
worse than redelivering a bad one until `max-redelivery-count` sends it there anyway.

When the registry is enabled and the application declares no `SolaceListenerErrorHandler`, a
`SchemaRegistryErrorHandler` is registered and given to the default container factory. To keep your own
handler as well, wrap it:

```java
@Bean
SolaceListenerErrorHandler errorHandler() {
    return new SchemaRegistryErrorHandler(new MyErrorHandler());
}
```

Two caveats:

- Per-message outcomes must be negotiated when a flow binds, so set
  `solace.listener.negative-acknowledgement: true` ([9](09-consuming-messages.md)). The configuration
  logs a warning when it's not set. Without it, the broker refuses the `REJECTED` settlement and the
  message is redelivered.
- Transactional containers ignore outcomes: the rollback redelivers until `max-redelivery-count`.

An outbound failure throws from `send`. Inside a transaction, that rolls the transaction back.

---

## 19.8 Configuration reference: `solace.schema-registry.*`

An unset property keeps the Apicurio default; only the two defaults in 19.6 differ.

| Property | Default | Meaning |
| :--- | :--- | :--- |
| `url` | none | Apicurio Registry REST endpoint. **Setting it enables the feature.** |
| `formats` | every format whose module is present | `AVRO`, `PROTOBUF`, `JSON_SCHEMA`. A listed format without its module fails startup. |
| `username`, `password` | none | HTTP basic authentication. |
| `oauth.token-endpoint`, `.client-id`, `.client-secret`, `.scope` | none | OAuth 2.0 client credentials, an alternative to basic authentication. |
| `tls.truststore-location`, `-password`, `-type` | Apicurio: type `JKS` | Trust store for a registry with a private CA. |
| `tls.keystore-location`, `-password`, `-type` | none | Key store for mutual TLS. |
| `tls.trust-all`, `tls.verify-host` | Apicurio: `false`, `true` | Development only, and keep on, respectively. |
| `http-adapter` | **`JDK`** | `JDK`, `VERTX`, `AUTO` (19.6). |
| `destinations` | empty = all | Topic expressions where POJO payloads are governed (19.3). |
| `require-schema-id` | `false` | Reject a message on a governed destination whose body isn't registry-framed. |
| `artifact-resolver-strategy` | `TOPIC_PROFILE` | `TOPIC_PROFILE`, `DESTINATION`, `TOPIC`, `RECORD`, or a class name (19.5). |
| `topic-profile[].topic-expression`, `.artifact-id` | none | Required per entry. |
| `topic-profile[].group-id`, `.version` | none | Optional. |
| `find-latest` | Apicurio: `false` | Resolve the latest artifact version. |
| `explicit-artifact.group-id`, `.artifact-id`, `.version` | none | Pin every serialisation to one artifact. |
| `auto-register` | Apicurio: `false` | Register unknown schemas on first use. **Development only.** |
| `auto-register-if-exists` | Apicurio: `FIND_OR_CREATE_VERSION` | `FAIL`, `CREATE_VERSION`, `FIND_OR_CREATE_VERSION`. |
| `use-id` | Apicurio: `CONTENT_ID` | `CONTENT_ID` or `GLOBAL_ID`, the id written into the body. |
| `dereference-schema` | Apicurio: `false` | Ask the registry for dereferenced schemas. |
| `cache.check-period` | Apicurio: `30s` | How long a resolved artifact is cached. |
| `cache.latest` | Apicurio: `true` | Cache "latest" lookups too. |
| `cache.fault-tolerant-refresh` | **`true`** | Keep a cached schema when refreshing it fails (19.6). |
| `cache.background-refresh` | Apicurio: `false` | Serve stale entries while refreshing in the background. |
| `retry.count`, `retry.backoff` | Apicurio: `3`, `300ms` | Registry request retries. |
| `avro.encoding` | Apicurio: `BINARY` | `BINARY` or `JSON`. |
| `avro.validate-writer-schema` | Apicurio: `true` | Check a record's own schema against the registry's before writing. |
| `protobuf.validation` | Apicurio: `true` | Check a message's descriptor against the registry schema before writing. |
| `protobuf.derive-class` | Apicurio: `false` | Deserialise to the class the schema's Java options name. |
| `json-schema.validation` | Apicurio: `true` | Validate on both sides. |
| `json-schema.schema-location` | none | Classpath schema, for auto-registration. |
| `properties.*` | none | Raw `apicurio.registry.*` keys for every format, applied after the above. |
| `avro.properties.*`, `protobuf.properties.*`, `json-schema.properties.*` | none | Raw keys for one format only, applied last. |

Startup validation rejects:

- no `url`;
- `TOPIC_PROFILE` with neither mappings nor an explicit artifact;
- a mapping without a `topic-expression` or an `artifact-id`;
- `RECORD` with any format other than Avro enabled.

Remember [5.1](05-configuration.md#a-yaml-trap-worth-knowing): a `schema-registry:` block with every
child commented out fails startup.

---

## 19.9 Classes

| Type | Purpose |
| :--- | :--- |
| `SchemaRegistrySolaceMessageConverter` | The converter: the routing in 19.3 and 19.4, over `SchemaCodecs`, with a fallback converter (Jackson by default). `setDestinations`, `setRequireSchemaId`, `isGoverned(name)`, `getCodecs()`. |
| `SchemaCodec` | One format's serde, in bytes: `getFormat`, `isSchemaPayload`, `producesType`, `serialize`, `deserialize`, `close`. The converter works only against this interface, which keeps Apicurio optional and the routing testable without a registry. |
| `SchemaCodecs` | The enabled codecs, one per format. `create(settings, objectMapper, classLoader)`, `of(codec...)`, `get(format)`, `forPayload`, `forTargetType`, `close`. |
| `ApicurioSchemaCodec` | Base for the Apicurio codecs: lazy serde creation, closes everything it created. |
| `AvroSchemaCodec`, `ProtobufSchemaCodec`, `JsonSchemaCodec` | The three formats. With the base class and `SolaceTopicProfileStrategy`, the only classes that import `io.apicurio`, Avro or Protobuf. |
| `SolaceTopicProfileStrategy` | Apicurio `ArtifactReferenceResolverStrategy` over Solace topic expressions. |
| `SchemaFormat` | `AVRO`, `PROTOBUF`, `JSON_SCHEMA`, each with its Apicurio artifact type and module. |
| `SchemaRegistryHeaders` | `SCHEMA_FORMAT` (`schemaFormat`), `MAGIC_BYTE`, `isFramed(byte[])`. |
| `SchemaRegistrySettings` | The bound settings, free of Apicurio types. `validate()`. |
| `SchemaRegistryConversionException` | `getReason()`; static `classify(message, cause)`. |
| `SchemaRegistryErrorHandler` | Rejects non-retryable schema failures and defers the rest to a delegate. |

Auto-configuration: `SolaceSchemaRegistryConfiguration`, imported by `SolaceAutoConfiguration` *before*
its own beans so that `solaceMessageConverter` backs off. It is conditional on
`solace.schema-registry.url`.

| Bean | Condition |
| :--- | :--- |
| `solaceSchemaCodecs` (`SchemaCodecs`) | missing bean |
| `solaceMessageConverter` (`SchemaRegistrySolaceMessageConverter`) | missing `SolaceMessageConverter` |
| `solaceSchemaRegistryErrorHandler` | missing `SolaceListenerErrorHandler` |

To plug in a custom codec, for another registry or a test fake, declare
`@Bean SchemaCodecs solaceSchemaCodecs() { return SchemaCodecs.of(myCodec); }`.

---

## 19.10 Not supported

| | Why |
| :--- | :--- |
| Other registries (Confluent, Solace Schema Registry) | Apicurio is the supported registry. Apicurio serves a Confluent-compatible API, and the `SchemaCodec` seam accepts other implementations. |
| Schema id in a header instead of the body | Apicurio's header mode exists only in its Kafka serdes. The framed body is the portable form. |
| `@SolaceListener(schema = …)` | The payload type and topic profile already decide the schema; an attribute would be a second source of truth. |
| Registering schemas from the application in production | `auto-register` is for development; register schemas from CI or the Apicurio UI. |

---

**Back to:** [README](../README.md)
