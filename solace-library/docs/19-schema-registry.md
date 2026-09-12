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
| `AVRO` | any Avro record: `GenericRecord` or a generated `SpecificRecord`; with `avro.datum-provider: REFLECT`, also a plain POJO on a governed destination | `GenericRecord`, the generated class, or (reflect) the POJO class | `apicurio-registry-serde-common-avro` |
| `PROTOBUF` | any `com.google.protobuf.Message`: a generated class or `DynamicMessage` | the generated class, `DynamicMessage`, `Message` | `apicurio-registry-serde-common-protobuf` |
| `JSON_SCHEMA` | a POJO on a governed destination, or a Jackson `JsonNode` anywhere | any POJO, or `JsonNode` | `apicurio-registry-serde-common-jsonschema` |

All three can be enabled at once. Outbound, the payload's type picks the format. Inbound, the message
says which format it is (19.4).

**Avro.** Two deserializers are created on demand: one with Apicurio's specific reader, for listeners
that ask for a generated `SpecificRecord`, and one for everything else, which yields `GenericRecord`.

**Avro with plain Java objects.** `avro.datum-provider: REFLECT` (or `REFLECT_ALLOW_NULL`, which makes
every field nullable) switches Apicurio to Avro *reflection*: a POJO is written with a schema derived
from its fields, and a reader instantiates the class the schema names. That is how an existing DTO
travels as Avro without generated code. Which destinations write POJOs as Avro rather than JSON Schema is
set per topic, by `format: AVRO` on the `topic-profile` mapping (19.5). The reading side needs the same
class, under the same name, on its classpath.

**Trusted classes.** Since 1.11.4, Avro only loads a class named in a schema if that class is trusted; this
check exists to stop a hostile schema from naming a dangerous class. Without trust, a DTO fails with
*"Forbidden cris.prs.messaging.Person! This class is not trusted to be included in Avro schemas"*. The
codec trusts every payload class it is given to send and every type a listener asks for, since the
application already uses those classes. For the types of nested fields, add their packages to
`avro.trusted-packages`. Avro's own JVM properties `org.apache.avro.SERIALIZABLE_PACKAGES` and
`SERIALIZABLE_CLASSES` keep working alongside.

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
type, it's used as it is.

JSON Schema is the one format whose schema **cannot be inferred from the data**, the way an Avro schema is
derived from a class and a Protobuf schema read from the generated descriptor. `auto-register` therefore
has nothing to register, and the artifact has to exist before the first message. Declare the schemas with
`registration.schemas` (19.5) and add `json-schema.properties[apicurio.registry.find-latest]: "true"` so
the serde resolves them by their coordinates instead of searching for a schema it cannot produce.
Apicurio's own `json-schema.schema-location` is the narrower alternative: one location per serde, so it
fits an application with a single JSON Schema payload, not a request and a reply.

---

## 19.3 What goes through the registry

### Outbound

| Payload | Destination | Result |
| :--- | :--- | :--- |
| `null`, `byte[]`, `String` | any | Plain, as without a registry |
| An Avro record, a Protobuf message or a `JsonNode` | any | That format's codec |
| Any other object | matches `destinations` | the POJO format of the first `topic-profile` mapping matching the destination that sets `format` — `AVRO` (needs a reflect datum provider) — else JSON Schema. A format that isn't enabled or can't write POJOs is a `TYPE_MISMATCH` failure |
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
Each mapping needs an `artifact-id`; `group-id` and `version` are optional. A mapping may also set
`format: AVRO` (or `JSON_SCHEMA`, the default): the format *POJO* payloads sent to its topics are written
in. Avro records and Protobuf messages always use their own format, so `PROTOBUF` is rejected there. A topic that no mapping
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

### Getting schemas into the registry

Resolution finds an artifact; something has to have put it there. There are three ways, and they are not
interchangeable:

| | What it publishes | When | Suitable for |
| :--- | :--- | :--- | :--- |
| `auto-register: true` | the schema Apicurio derived from the payload | the first serialisation | development, Avro and Protobuf only |
| `registration.schemas` | a schema file you wrote, from a Spring resource location | startup or the first message, see below | any format, any environment |
| `registration.include-topic-profile` | the Avro or Protobuf schema derived from a `topic-profile` mapping's `payload-class` | startup or the first message, as `mode` says | Avro and Protobuf, when you would rather register at initialization than on the first message |
| outside the application | whatever CI or the registry UI publishes | before deployment | production |

**`auto-register` cannot do JSON Schema.** Apicurio derives an Avro schema from the class and reads a
Protobuf schema out of the generated descriptor, but `JsonSchemaParser.supportsExtractSchemaFromData()` is
`false` — there is nothing to derive a JSON Schema from. With `auto-register` on and no schema to register,
the resolver falls through to looking the artifact up by its coordinates, so for that format the artifact
must exist beforehand or every message fails.

**Declaring schemas.** List them, and the library publishes them:

```yaml
solace:
  schema-registry:
    registration:
      mode: STARTUP           # or FIRST_MESSAGE (default)
      fail-fast: false        # default
      if-exists: FIND_OR_CREATE_VERSION   # default
      schemas:
        - artifact-id: order
          group-id: orders
          format: JSON_SCHEMA
          location: "classpath:schemas/order.json"
```

`location` is any Spring resource location — `classpath:`, `file:`. `format` is the schema's language and
its Apicurio artifact type, so an `.avsc` under `format: AVRO` and a `.proto` under `format: PROTOBUF`
publish the same way. `version` is optional; without one the registry assigns the next.
`if-exists: FIND_OR_CREATE_VERSION` makes a restart with unchanged content find the existing version
rather than pile up new ones.

**`mode`: when they are published.**

| | `FIRST_MESSAGE` (default) | `STARTUP` |
| :--- | :--- | :--- |
| Publishes | just before the first conversion that uses the registry | as the registrar bean initialises |
| Registry down at boot | the instance starts | the instance starts (unless `fail-fast`) |
| A bad schema shows up | on the first request | at boot |

`FIRST_MESSAGE` is the default because it matches what the rest of this feature does: the serdes are built
lazily too, so an instance starts while the registry is down and a rolling restart is not blocked by it
(19.6). `STARTUP` moves the attempt earlier, which is what you want when a schema that the registry
rejects should stop a deployment rather than surface as a failed request. It is an *earlier* attempt, not
the only one: the converter still checks before the first conversion, so a startup attempt that failed
with `fail-fast` off is retried on the first message.

Registration happens once per instance — the first attempt that succeeds, and every later call is a flag
read. A failed attempt registers nothing, so the next one tries again.

**`fail-fast`** (off by default) lets a failure propagate instead of being logged: the context fails to
start under `STARTUP`, and the send or receive fails under `FIRST_MESSAGE`. Turn it on when a deployment
should not go live with a schema the registry would not take; leave it off when an instance must start
whatever the registry is doing.

**Registering `topic-profile` schemas.** `registration.schemas` needs a file per artifact. For Avro and
Protobuf that file is redundant &mdash; the schema is already derivable, from the class (Avro) or the
generated descriptor (Protobuf), which is exactly what `auto-register` does lazily on the first message.
`registration.include-topic-profile: true` registers those at initialization instead: give each
`topic-profile` mapping a `payload-class`, and the library derives and publishes its schema alongside the
declared ones, under the same `mode`, `fail-fast` and `if-exists`.

```yaml
solace:
  schema-registry:
    registration:
      mode: STARTUP
      include-topic-profile: true
    topic-profile:
      - topic-expression: "request-reply/quote-avro/request"
        artifact-id: quote-avro-request
        format: AVRO                                 # POJO written as Avro; needs a reflect datum provider
        payload-class: cris.prs.messaging.Person
      - topic-expression: "request-reply/quote-protobuf/request"
        artifact-id: quote-protobuf-request          # no format: a generated message is Protobuf
        payload-class: cris.prs.messaging.proto.QuoteRequest
```

The format is resolved from the mapping and the class: `format: AVRO` derives an Avro schema by reflection
(so `avro.datum-provider` must be `REFLECT` or `REFLECT_ALLOW_NULL`, or the class a generated
`SpecificRecord`); a generated `com.google.protobuf.Message` derives a Protobuf schema from its descriptor.
A mapping that resolves to **JSON Schema** &mdash; a POJO with no `format` &mdash; is **skipped**, because
that format cannot be inferred; declare those under `registration.schemas`. A mapping with no
`payload-class` is skipped too. When an artifact is both declared and derivable, the declared file wins.

Deriving a schema contacts neither a message nor the registry, so an instance still starts while the
registry is down; only the publish step needs it, exactly as for declared schemas.


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
| `topic-profile[].format` | `JSON_SCHEMA` | The format POJOs sent to the mapping's topics use: `JSON_SCHEMA` or `AVRO` (19.2). |
| `topic-profile[].payload-class` | none | Fully qualified class whose Avro or Protobuf schema is registered for the mapping's artifact when `registration.include-topic-profile` is on (19.5). |
| `find-latest` | Apicurio: `false` | Resolve the latest artifact version. |
| `explicit-artifact.group-id`, `.artifact-id`, `.version` | none | Pin every serialisation to one artifact. |
| `auto-register` | Apicurio: `false` | Register unknown schemas on first use. **Development only.** |
| `auto-register-if-exists` | Apicurio: `FIND_OR_CREATE_VERSION` | `FAIL`, `CREATE_VERSION`, `FIND_OR_CREATE_VERSION`. |
| `registration.mode` | `FIRST_MESSAGE` | When declared schemas are published: `FIRST_MESSAGE` or `STARTUP` (19.5). |
| `registration.include-topic-profile` | `false` | Also register Avro/Protobuf schemas derived from `topic-profile` mappings that name a `payload-class`, under the same `mode`/`fail-fast`/`if-exists` (19.5). |
| `registration.fail-fast` | `false` | Let a failed publish fail startup or the conversion, instead of warning and retrying. |
| `registration.if-exists` | `FIND_OR_CREATE_VERSION` | `FAIL`, `CREATE_VERSION`, `FIND_OR_CREATE_VERSION`. |
| `registration.schemas[].artifact-id`, `.format`, `.location` | none | Required per entry. `location` is any Spring resource location. |
| `registration.schemas[].group-id`, `.version` | none | Optional. |
| `use-id` | Apicurio: `CONTENT_ID` | `CONTENT_ID` or `GLOBAL_ID`, the id written into the body. |
| `dereference-schema` | Apicurio: `false` | Ask the registry for dereferenced schemas. |
| `cache.check-period` | Apicurio: `30s` | How long a resolved artifact is cached. |
| `cache.latest` | Apicurio: `true` | Cache "latest" lookups too. |
| `cache.fault-tolerant-refresh` | **`true`** | Keep a cached schema when refreshing it fails (19.6). |
| `cache.background-refresh` | Apicurio: `false` | Serve stale entries while refreshing in the background. |
| `retry.count`, `retry.backoff` | Apicurio: `3`, `300ms` | Registry request retries. |
| `avro.encoding` | Apicurio: `BINARY` | `BINARY` or `JSON`. |
| `avro.validate-writer-schema` | Apicurio: `true` | Check a record's own schema against the registry's before writing. |
| `avro.trusted-packages` | none | Packages Avro may instantiate classes from, beyond payload and listener types (trusted automatically). |
| `avro.datum-provider` | Apicurio: `DEFAULT` | `DEFAULT` (generated and generic records), `REFLECT` or `REFLECT_ALLOW_NULL` (plain POJOs too). |
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
- `RECORD` with any format other than Avro enabled;
- a mapping with `format: PROTOBUF`, or `format: AVRO` without a reflect datum provider.
- a `registration.schemas` entry without an `artifact-id`, a `format` or a `location`.

Remember [5.1](05-configuration.md#a-yaml-trap-worth-knowing): a `schema-registry:` block with every
child commented out fails startup.

---

## 19.9 Classes

| Type | Purpose |
| :--- | :--- |
| `SchemaRegistrySolaceMessageConverter` | The converter: the routing in 19.3 and 19.4, over `SchemaCodecs`, with a fallback converter (Jackson by default). `setDestinations`, `setRequireSchemaId`, `setPojoFormats`, `pojoFormatFor(name)`, `isGoverned(name)`, `getCodecs()`. |
| `SchemaCodec` | One format's serde, in bytes: `getFormat`, `isSchemaPayload`, `acceptsPojos`, `producesType`, `serialize`, `deserialize`, `close`, and `canDeriveSchema`/`deriveSchema(Class)` for registering a schema at initialization. The converter works only against this interface, which keeps Apicurio optional and the routing testable without a registry. |
| `SchemaCodecs` | The enabled codecs, one per format. `create(settings, objectMapper, classLoader)`, `of(codec...)`, `get(format)`, `forPayload`, `forTargetType`, `close`. |
| `ApicurioSchemaCodec` | Base for the Apicurio codecs: lazy serde creation, closes everything it created. |
| `AvroSchemaCodec`, `ProtobufSchemaCodec`, `JsonSchemaCodec` | The three formats. With the base class and `SolaceTopicProfileStrategy`, the only classes that import `io.apicurio`, Avro or Protobuf. |
| `SolaceTopicProfileStrategy` | Apicurio `ArtifactReferenceResolverStrategy` over Solace topic expressions. |
| `SchemaFormat` | `AVRO`, `PROTOBUF`, `JSON_SCHEMA`, each with its Apicurio artifact type and module. |
| `SchemaRegistryHeaders` | `SCHEMA_FORMAT` (`schemaFormat`), `MAGIC_BYTE`, `isFramed(byte[])`. |
| `SchemaArtifactRegistrar` | Publishes declared schemas and, with `registration.include-topic-profile`, schemas derived from `topic-profile` mappings, at startup or on first use: `hasSchemas()`, `hasDerivedSchemas()`, `hasWork()`, `isRegistered()`, `registerOnce()`, `register()`. |
| `SchemaRegistrySettings` | The bound settings, free of Apicurio types. `validate()`. |
| `SchemaRegistryConversionException` | `getReason()`; static `classify(message, cause)`. |
| `SchemaRegistryErrorHandler` | Rejects non-retryable schema failures and defers the rest to a delegate. |

Auto-configuration: `SolaceSchemaRegistryConfiguration`, imported by `SolaceAutoConfiguration` *before*
its own beans so that `solaceMessageConverter` backs off. It is conditional on
`solace.schema-registry.url`.

| Bean | Condition |
| :--- | :--- |
| `solaceSchemaCodecs` (`SchemaCodecs`) | missing bean |
| `solaceSchemaArtifactRegistrar` (`SchemaArtifactRegistrar`) | missing bean |
| `solaceMessageConverter` (`SchemaRegistrySolaceMessageConverter`) | missing `SolaceMessageConverter` |
| `solaceSchemaRegistryErrorHandler` | missing `SolaceListenerErrorHandler` |

To plug in a custom codec, for another registry or a test fake, declare
`@Bean SchemaCodecs solaceSchemaCodecs() { return SchemaCodecs.of(myCodec); }`.

---

## 19.10 The demo in this repository

The quote service runs in all three formats in the `client` and `server` modules, against a dev Apicurio
Registry. Every one answers the same question — a `Person` in, a `Quote` out — so only the wire format
differs:

- **Avro:** `QuoteAvroConsumer` takes the shared `Person` and returns the shared `Quote`, written by
  reflection. Its topics are mapped with `format: AVRO`, and the schemas appear on the first request,
  since the demo registry has `auto-register: true`.
- **Protobuf:** `QuoteProtobufConsumer` takes the generated `QuoteRequest` and returns the generated
  `QuoteReply`, from `shared-proto/src/main/proto/quote.proto`, mapped to and from the DTOs by
  `QuoteProtoMapper`. Its mappings name no format: a generated message is Protobuf wherever it is sent.
- **JSON Schema:** `QuoteJsonSchemaConsumer` takes the shared `Person` and returns the shared `Quote`
  again, as ordinary JSON with `validation: true`. Its mappings name no format either — a governed topic
  whose mapping is silent is JSON Schema. Because the schema cannot be inferred (19.2), the two schemas
  live beside the DTOs they describe, in `shared-dto/src/main/resources/schemas/`, and both applications
  declare them under `registration.schemas` with `mode: STARTUP`, so the library publishes them into the
  `solace-request-reply` group as each starts; `json-schema.properties[apicurio.registry.find-latest]`
  then makes the serdes resolve them by coordinates. `fail-fast` is left off, so an unreachable registry
  is a warning and the first request tries again.
- **Governed destinations:** `request-reply/quote-avro/>` and `request-reply/quote-jsonschema/>` — the
  only places a POJO is written through the registry. Every other exchange in the repository keeps sending
  plain, unvalidated JSON.
- **Reply destinations:** each demo has its own, `request-reply/quote-<format>/reply/<pod>`, mapped with
  `>` so one artifact covers every client instance.

The endpoints are `GET /request-reply/quote-avro/send`, `/request-reply/quote-protobuf/send` and
`/request-reply/quote-jsonschema/send`, each with `send-multiple` and `send-batch` alongside. See the
repository README.

---

## 19.11 Not supported

| | Why |
| :--- | :--- |
| Other registries (Confluent, Solace Schema Registry) | Apicurio is the supported registry. Apicurio serves a Confluent-compatible API, and the `SchemaCodec` seam accepts other implementations. |
| Schema id in a header instead of the body | Apicurio's header mode exists only in its Kafka serdes. The framed body is the portable form. |
| `@SolaceListener(schema = …)` | The payload type and topic profile already decide the schema; an attribute would be a second source of truth. |
| Registering schemas from the application in production | `auto-register` is for development; register schemas from CI or the Apicurio UI. |

---

**Back to:** [README](../README.md)
