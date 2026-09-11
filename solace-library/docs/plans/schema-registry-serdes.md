# Plan: Schema Registry SERDES

*Status: **implemented, on Apicurio Registry** — see [19. Schema Registry](../19-schema-registry.md).*

> **Revision — registry switched to Apicurio.** This plan was written for Solace Schema Registry SERDES
> (Avro and JSON Schema). As built, the library uses **Apicurio Registry** and supports **Apache Avro,
> Google Protocol Buffers and JSON Schema** explicitly, through Apicurio's generic, Kafka-free serde modules
> `io.apicurio:apicurio-registry-serde-common-{avro,protobuf,jsonschema}` 3.3.x. What changed from the
> sections below:
>
> - **No JCSMP 10.28 pin.** That existed only for Solace's `SerdeMessage`; Apicurio works on bytes, so the
>   library stays on the starter BOM's JCSMP.
> - **The schema id travels in the body**, in Apicurio's standard framing `[0x00][id][payload]`, not in an
>   SDT user property. Detection is by the magic byte. The format is written as a `schemaFormat` user
>   property, which is what makes **all three formats usable at once**: outbound the payload type picks the
>   format, inbound the property does (then the listener type, then JSON Schema).
> - **`SchemaCodec` became byte-level** (`serialize(destination, payload) → byte[]`), with a `SchemaCodecs`
>   bean holding one codec per enabled format; `formats` defaults to every format whose module is present.
> - **Artifact resolution**: the default `TOPIC_PROFILE` is this library's own `SolaceTopicProfileStrategy`,
>   an Apicurio `ArtifactReferenceResolverStrategy` over Solace wildcard expressions. `DESTINATION`,
>   `TOPIC` and `RECORD` map to Apicurio's strategies.
> - **Library defaults that differ from Apicurio's:** `cache.fault-tolerant-refresh: true` (the plan's
>   `use-cached-on-error`), and `http-adapter: JDK` so no Vert.x event loop runs per serde.
> - **Protobuf** decodes to `DynamicMessage` and re-parses into the listener's generated class; **JSON
>   Schema** decodes to `JsonNode` mapped with the application's `ObjectMapper` — both because Apicurio's
>   fixed return class is one class per deserializer.
> - The Apicurio calls were written against the Apicurio 3.3.3 **source**; the first real build confirms
>   them against the jars.
>
> Unchanged from the plan: the destination-aware converter SPI, converter-owned user properties, the
> error-handler bean fix, the failure reasons and `SchemaRegistryErrorHandler`, lazy creation, and
> fail-fast startup.

*Originally: implements [18. Feature backlog](../18-feature-backlog.md), Tier 3 — "Solace Schema
Registry SERDES (10.28+)". The backlog rated it **M**; this plan agrees, with one small core SPI change
and one latent bug fix as prerequisites.*

**Deviations from the plan, as built:**

- The seam to SERDES is a `SchemaCodec` interface rather than a `SerdesFactory`, so the converter and its
  tests never load a SERDES class. `AvroSchemaCodec` and `JsonSchemaCodec` are the only SERDES users.
- No `@ConditionalOnClass` on the SERDES jars: a configured URL with a missing jar or old JCSMP fails
  startup from the codec bean, naming the fix, instead of silently falling back to plain JSON.
- JCSMP is pinned with an explicit `dependencyManagement` entry in **every** module, not only a direct
  dependency in the library: the starter BOM would otherwise downgrade it transitively in `client` and
  `server` (AGENTS.md #29).
- Unrecognised SERDES failures are `UNCLASSIFIED` and retryable, so they defer to the container rather
  than being rejected.

---

## 1. Goal

Let an application exchange **schema-governed** payloads — Avro or JSON Schema, validated against and
versioned in [Solace Schema Registry](https://docs.solace.com/Schema-Registry/schema-registry-overview.htm) —
through the existing `SolaceTemplate`, `ReplyingSolaceTemplate` and `@SolaceListener` programming
model, with **no change to application code** beyond configuration and payload types. The Spring for
Apache Kafka analogue is swapping `JsonSerializer` for a registry-aware serializer.

Non-goals are listed in [§9](#9-out-of-scope).

---

## 2. What Solace SERDES is (as documented)

| Fact | Source |
| :--- | :--- |
| Formats: **Avro** and **JSON Schema**. No Protobuf. | SERDES overview |
| Artifacts: `com.solace:solace-schema-registry-avro-serde`, `…-jsonschema-serde`, BOM `…-serdes-bom`, all **1.0.0** | JCSMP SERDES page |
| JCSMP glue: `com.solacesystems.jcsmp.serialization.SerdeMessage` — **JCSMP 10.28+** | JCSMP javadoc |
| `SerdeMessage.serialize(Serializer<T>, [Destination,] BytesXMLMessage, T)` writes the **binary attachment** and the schema id as **SMF user properties** | JCSMP javadoc |
| `SerdeMessage.deserialize(Deserializer<T>, [Destination,] BytesXMLMessage)` | JCSMP javadoc |
| Serializers: `AvroSerializer<T>`, `JsonSchemaSerializer<T>`; both extend `AbstractSerializer<T,S> implements com.solace.serdes.Serializer<T>, Closeable` with `configure(Map<String,?>)`, `serialize(String destinationName, T, Map<String,Object> headers)`, `close()` (which closes "open Vertx connections") | SERDES javadoc |
| Schema id lives in a **header**, not the payload: `SerdeHeaders.SCHEMA_ID` (long, default) or `SCHEMA_ID_STRING`, chosen by `SerdeProperties.SCHEMA_HEADER_IDENTIFIERS` | SERDES javadoc |
| Artifact resolution: `DestinationIdStrategy` (artifactId = destination name, default group), `SolaceTopicIdStrategy` + `SolaceTopicProfile` (Solace wildcard expression → artifact), `RecordIdStrategy` (Avro record full name) | SERDES javadoc |
| Avro deserializer: `AvroProperties.RECORD_TYPE` = `GENERIC_RECORD` (default) or `SPECIFIC_RECORD` | SERDES javadoc |
| JSON Schema deserializer to a POJO: `javaType` schema property (`JsonSchemaProperties.TYPE_PROPERTY`) or `SerdeProperties.DESERIALIZED_TYPE` — **one type per deserializer instance** | JCSMP tutorial, SERDES javadoc |
| Registry: `REGISTRY_URL` (e.g. `http://host:8081/apis/registry/v3`), basic auth, trust store, `AUTO_REGISTER_ARTIFACT` (default `false`), `FIND_LATEST_ARTIFACT`, cache TTL 30 s, `USE_CACHED_ON_ERROR` (default `false`), 3 request attempts, 500 ms backoff | `SchemaResolverProperties` |
| The registry itself ships as Docker/Helm (backend, UI, PostgreSQL, IdP; roles `sr-developer`, `sr-readonly`), downloaded with a Solace ID | Helm deployment page |

**Not verified against the jar** — Maven Central is blocked from both the cloud workspace and this
machine's shell, so per [hard-won constraint #23](../../../AGENTS.md) nothing above is taken as a
signature until Phase 0 confirms it. In particular the **string values** of `SerdeHeaders.SCHEMA_ID`
/ `SCHEMA_ID_STRING`, the exception type SERDES throws, and serializer thread safety are unknown.

---

## 3. What the current code gets in the way of

Reading `solace-library` against the SERDES model turns up seven things. The first four are defects the
feature would hit on day one; they decide the design.

### 3.1 The converter never sees the destination

```java
XMLMessage toMessage(Object payload);          // SolaceMessageConverter today
```

Both destination-driven strategies (`DestinationIdStrategy`, `SolaceTopicIdStrategy`) resolve the
schema **from the topic being published to**. `SolaceTemplate.send(destination, payload, headers)`
knows it, but `createMessage(payload, headers)` drops it before calling the converter. Inbound is fine
— `BytesXMLMessage.getDestination()` is on the message.

### 3.2 The header mapper would overwrite the schema id

`SolaceTemplate.createMessage` runs the converter **first** and the header mapper **second**, and
`DefaultSolaceHeaderMapper.fromHeaders` merges into the message's existing `SDTMap`. Today no converter
writes user properties, so order never mattered. With SERDES it does: a listener that returns
`MessageBuilder.withPayload(reply).copyHeaders(inbound.getHeaders())` carries the **request's** schema
id header (inbound `toHeaders` copies every user property), `sanitize` does not strip it, and the mapper
overwrites the reply's correct schema id with the request's. The consumer then decodes the reply
against the wrong schema — silently, if the two happen to be field-compatible.

### 3.3 The resolved JCSMP is too old

`AGENTS.md` records JCSMP **10.27.2** from `solace-spring-boot-bom:2.5.0`; `SerdeMessage` needs
**10.28+**. `gradle/libs.versions.toml` already declares `solaceJcsmp` at 10.30.1 but no module uses it.

### 3.4 Per-instance reply topics break `DestinationIdStrategy`

[Rule #1](../../../AGENTS.md#-rules-to-preserve): replies go to `<reply-topic-prefix>/<instance-id>`.
Under `DestinationIdStrategy` every pod's reply topic is a different artifact id — with auto-register
that is **one artifact per pod, forever**, and without it every reply fails to serialise. Request topics
are no better: several listeners subscribe with `…/>`. The strategy that fits this library is
`SolaceTopicIdStrategy` with wildcard mappings (`app/reply/>`), or `RecordIdStrategy` for Avro.

### 3.5 One converter serves the whole application

Template, every container, request-reply and the browser share **one** `SolaceMessageConverter`. An
application adopting the registry typically does so for *some* destinations. Avro payloads are
self-identifying (`GenericRecord`/`SpecificRecord`), but a JSON Schema POJO is indistinguishable from
one the Jackson converter should handle — so outbound needs a **destination rule**, and inbound can
detect by the **presence of the schema id header**.

### 3.6 A declared `SolaceListenerErrorHandler` bean is ignored (latent bug)

[14.1](../14-extension-points.md#141-the-map) says an error handler is replaced by declaring a bean.
It is not: `SolaceAutoConfiguration.solaceListenerContainerFactory` never looks one up, and nothing else
does either (`grep` finds only the factory's setter). Only a hand-built factory gets one. This matters
here because schema failures are the textbook case for per-failure outcomes (§5.5).

### 3.7 The registry sits on hot paths

- **Inbound** conversion runs on the JCSMP delivery thread for `INLINE` dispatch — which is every
  transactional container ([#10](../../../AGENTS.md)). A registry lookup there stalls the flow.
- **Outbound** inside `@Transactional` holds the transacted session while it calls the registry.
- With `USE_CACHED_ON_ERROR=false` (the SERDES default) and a 30 s TTL, a registry outage fails every
  message within 30 s — broker healthy, application healthy, all traffic `FAILED`.

Caching makes the first two a cold-start cost; the third needs a different default (§6).

### 3.8 Dependency weight

`close()` mentions Vert.x, so the registry client brings Vert.x and hence Netty. The client is a WebFlux
app on Reactor Netty whose Netty version Spring Boot manages. Must be checked, and the SERDES jars must
stay **optional** exactly as Micrometer is.

---

## 4. Design

### 4.1 Shape

```
core/SolaceMessageConverter          + default toMessage(Object payload, String destination)
core/SolaceTemplate                  + createMessage(String destination, Object, Map) — the old one delegates
core/DefaultSolaceHeaderMapper       user properties written by the converter win over headers
schema/                              NEW — the only package that imports com.solace.serdes.*
  SchemaRegistrySolaceMessageConverter
  SchemaFormat                       AVRO | JSON_SCHEMA
  SchemaRegistryConversionException  extends SolaceMessagingException, carries a Reason
  SchemaRegistryErrorHandler         decorator: REJECTED for non-retryable reasons
  SerdesFactory                      builds configured Serializer/Deserializer instances (test seam)
autoconfigure/SolaceSchemaRegistryConfiguration   imported by SolaceAutoConfiguration
autoconfigure/SolaceProperties.SchemaRegistry      solace.schema-registry.*
```

Mirrors `observability`: a package that is the sole importer of an optional library, and a
configuration class whose every bean is conditional. `core`, `listener` and `requestreply` stay free of
SERDES types.

### 4.2 Core SPI change (additive)

```java
public interface SolaceMessageConverter {
    XMLMessage toMessage(Object payload);

    /** Destination-aware variant; the default ignores the destination. */
    default XMLMessage toMessage(Object payload, String destination) {
        return toMessage(payload);
    }
    Object fromMessage(BytesXMLMessage message, Class<?> targetType);
}
```

A `default` method, the same technique as `SolaceSessionFactory.isHealthy()`, so every custom converter
compiles unchanged. `SolaceTemplate.send(String, T, Map)` and `send(Message<?>)` call a new
`createMessage(destination, payload, headers)`; the existing public `createMessage(payload, headers)`
delegates with `null`. `ReplyingSolaceTemplate` and listener replies already funnel through
`send(String, T, Map)` (`ReplyingSolaceTemplate:181`, `AbstractSolaceListenerAdapter.handleResult`), so
requests and replies both get the destination with no further change.

Rejected alternative: a thread-local "current destination". Invisible coupling, and wrong the moment a
converter is called outside `send`.

### 4.3 Header precedence (fixes 3.2)

`DefaultSolaceHeaderMapper.fromHeaders` skips a user-property header whose name **is already present in
the message's `SDTMap`** — i.e. was written by the converter — and logs it at debug. Generic, needs no
knowledge of SERDES header names, and a no-op for every existing converter since none writes
properties. Rule: *the converter owns what it wrote*. Becomes hard-won constraint #27 in `AGENTS.md`.

### 4.4 Outbound algorithm

```
toMessage(payload, destination):
  payload is null | byte[] | String                → fallback (Jackson converter, pass-through as today)
  format AVRO and payload is an Avro record        → SERDES   (self-identifying, whatever the destination)
  destination does not match `destinations`        → fallback
  format AVRO (non-record payload)                 → SchemaRegistryConversionException(TYPE_MISMATCH)
  format JSON_SCHEMA                               → node = payload instanceof JsonNode
                                                            ? payload : objectMapper.valueToTree(payload)
                                                     SerdeMessage.serialize(serializer, topic(destination), msg, node)
```

`destinations` is a list of Solace topic expressions matched with the existing `SolaceTopicMatcher`;
empty means *all*. **Why convert POJOs to `JsonNode` ourselves:** the application's `ObjectMapper`
(naming strategy, modules, dates) produces the same JSON it does for the Jackson converter, so switching a
destination to the registry changes *validation*, not the wire shape.

### 4.5 Inbound algorithm

```
fromMessage(message, targetType):
  targetType null | Object | BytesXMLMessage       → raw message        (unchanged contract)
  targetType byte[] | String                       → raw body, no registry call
  message has no schema id header:
      strict and destination matches `destinations` → SchemaRegistryConversionException(MISSING_SCHEMA_ID)
      otherwise                                     → fallback
  AVRO:  GenericRecord target                      → generic deserializer
         SpecificRecord subtype                    → specific deserializer (RECORD_TYPE=SPECIFIC_RECORD),
                                                     then check targetType.isInstance
         anything else                             → TYPE_MISMATCH
  JSON_SCHEMA: node = SerdeMessage.deserialize(jsonNodeDeserializer, message)
               targetType is JsonNode ? node : objectMapper.treeToValue(node, targetType)
```

The `JsonNode` bridge is the key decision on inbound: `DESERIALIZED_TYPE` is fixed per deserializer
instance, but `targetType` varies per listener and per topic-dispatch target. One validating
deserializer plus the application's mapper handles every type, needs no `javaType` in schemas, and keeps
`shared-dto` free of any dependency — the DTOs work as they are.

Everything that uses the converter inherits this: listeners, topic dispatch (each target keeps its own
type), request-reply replies, and `SolaceOperations.browse` — so a DMQ of schema-rejected messages can be
inspected with the same types.

### 4.6 Artifact resolution (fixes 3.4)

`solace.schema-registry.artifact-resolver-strategy`:

| Value | Maps to | Use |
| :--- | :--- | :--- |
| `TOPIC_PROFILE` **(default)** | `SolaceTopicIdStrategy` + a `SolaceTopicProfile` built from `topic-profile` | Wildcards, per-instance reply topics — the only one that fits every exchange pattern |
| `RECORD` | `RecordIdStrategy` | Avro only; schema follows the record type, not the topic |
| `DESTINATION` | `DestinationIdStrategy` | Only fixed, non-per-instance topics. Startup **warning** if request-reply is enabled |
| a class name | passed through | Anything else |

```yaml
solace:
  schema-registry:
    topic-profile:
      - topic-expression: "orders/place"
        artifact-id: order-request
      - topic-expression: "app/reply/>"      # every instance's reply topic, one artifact
        artifact-id: order-reply
```

The docs must say the rule plainly: **map the reply prefix with `>`**, never the resolved reply topic.

**A shared reply destination carries several reply types.** The demo's per-instance destination
`request-reply/reply-1/<pod>` receives both `Person` and `Quote` replies, so no single topic mapping is
right for it. Three ways out, in order of preference:

1. **Avro:** `RECORD` — the schema follows the record, and one destination can carry any number of types.
2. **JSON Schema:** give the schema-governed service its own reply destination with a `ReplyEndpointSpec`
   (as `inventoryReplyingSolaceTemplate` does). This becomes a **fifth** reason to split a reply
   destination out, alongside the four in `AGENTS.md`.
3. Leave replies outside `destinations`, so they travel as plain JSON while requests are governed.

### 4.7 Configuration — `solace.schema-registry.*`

| Property | Default | Maps to / note |
| :--- | :--- | :--- |
| `url` | — | `REGISTRY_URL`. **Its presence enables the feature** |
| `format` | — | `AVRO` \| `JSON_SCHEMA`. Required; one per application in v1 |
| `username`, `password` | — | `AUTH_USERNAME`, `AUTH_PASSWORD` |
| `trust-store.path`, `trust-store.password`, `validate-certificate` | —, —, `true` | TLS |
| `destinations` | empty = all | Topic expressions for outbound selection and strict inbound |
| `require-schema-id` | `false` | Strict inbound (§4.5) |
| `artifact-resolver-strategy` | `TOPIC_PROFILE` | §4.6 |
| `topic-profile[]` | — | `topic-expression`, `artifact-id`, optional `group-id`, `version` |
| `find-latest`, `explicit-artifact.*` | SERDES | Lookup |
| `auto-register`, `auto-register-if-exists` | `false`, `FIND_OR_CREATE_VERSION` | Documented as **dev only** |
| `schema-header-id` | `SCHEMA_ID` | `SCHEMA_ID_STRING` for readable ids in message dumps |
| `cache.ttl`, `cache.latest`, `cache.use-cached-on-error` | `30s`, `true`, **`true`** | Library default differs from SERDES — see 3.7 |
| `request.attempts`, `request.backoff` | `3`, `500ms` | `Duration`, converted at the boundary, as `ackTimer` is |
| `avro.encoding`, `avro.dereferenced-schema` | `BINARY`, `false` | |
| `json-schema.validate`, `json-schema.schema-location` | `true`, — | |
| `properties.*` | — | Raw SERDES keys, passed through last — the escape hatch |

Every scalar is a nullable boxed type written only when set, like `ContainerProperties.Flow`, so an
untouched property keeps the SERDES default (except the one deliberately changed above). Remember
[#21](../../../AGENTS.md): `schema-registry:` with every child commented out fails startup.

### 4.8 Auto-configuration

`SolaceSchemaRegistryConfiguration` in `org.cris.prs.solace.autoconfigure` (never under
`cris.prs.messaging` — [#1](../../../AGENTS.md)), `@Import`ed by `SolaceAutoConfiguration` **ahead of**
its own bean methods:

- `@ConditionalOnClass(name = {"com.solace.serdes.Serializer",
  "com.solacesystems.jcsmp.serialization.SerdeMessage"})` — string names, since both are optional; the
  second catches an application that added the SERDES jar but still resolves JCSMP < 10.28, with a
  startup log naming the fix instead of a `NoClassDefFoundError` on first send.
- `@ConditionalOnProperty("solace.schema-registry.url")`.
- Declares `solaceMessageConverter` (`@ConditionalOnMissingBean`) wrapping a `JacksonSolaceMessageConverter`
  over the application's `ObjectMapper` as the fallback. A user-declared converter still wins.
- Declares `solaceSchemaRegistryErrorHandler` only if no `SolaceListenerErrorHandler` bean exists.
- No class-level `@ConditionalOnBean`.

Relying on `@Import` order is the fragile part, so an `ApplicationContextRunner` test pins it: property
absent → Jackson converter; present → schema converter; user bean → user bean.

The converter implements `AutoCloseable`; Spring infers the destroy method, and containers — being
`SmartLifecycle` — have stopped before singletons are destroyed, so no message is converted after
`close()`.

### 4.9 Dependencies

```groovy
// solace-library/build.gradle
api libs.solaceJcsmp                         // >= 10.28 for SerdeMessage; verify it beats the BOM
compileOnly platform(libs.solaceSerdesBom)
compileOnly libs.solaceAvroSerde
compileOnly libs.solaceJsonSchemaSerde
testImplementation platform(libs.solaceSerdesBom)
testImplementation libs.solaceAvroSerde
testImplementation libs.solaceJsonSchemaSerde
testImplementation 'org.springframework.boot:spring-boot-test'   // ApplicationContextRunner
```

Applications add the one serde artifact they use. `02-getting-started.md` shows it.

---

## 5. Failure model

### 5.1 `SchemaRegistryConversionException.Reason`

| Reason | Example | Retryable | Outcome from `SchemaRegistryErrorHandler` |
| :--- | :--- | :--- | :--- |
| `REGISTRY_UNAVAILABLE` | connection refused, 5xx, timeout after attempts | yes | defer to container (`FAILED` → redelivery) |
| `SCHEMA_NOT_FOUND` | id in header unknown to this registry | no | `REJECTED` |
| `VALIDATION_FAILED` | payload does not match its schema | no | `REJECTED` |
| `MISSING_SCHEMA_ID` | strict mode, no header | no | `REJECTED` |
| `TYPE_MISMATCH` | Avro record is not the listener's type | no | `REJECTED` |

A retry cannot make a malformed payload valid, so rejecting it early — straight to the DMQ — is the same
reasoning as topic dispatch's unmatched-message rule, applied in the other direction.

### 5.2 Wiring caveats

- `SchemaRegistryErrorHandler` decides per message, so it needs
  `solace.listener.negative-acknowledgement: true` ([#13](../../../AGENTS.md)). The configuration logs a
  **startup warning** when the registry is enabled, the handler is in use, and that flag is not `true`.
- Transactional containers ignore outcomes; there a rollback redelivers until `max-redelivery-count`.
  Documented, not worked around.
- Outbound failures throw from `send` — inside a transaction that rolls it back, which is correct.

---

## 6. Operational defaults and observability

- **`cache.use-cached-on-error: true`** by default. A registry outage then degrades to "no new schemas"
  instead of "no messages". This is the one place the library overrides a SERDES default, and the docs
  say so.
- **No registry health indicator in v1.** `SolaceHealthIndicator` never makes a network call, by design,
  and a readiness probe that fails when the registry blips would take every pod out of rotation at once
  — the outcome the cache default exists to prevent. Revisit as an opt-in if asked for.
- **Metrics (Phase 4, optional):** `solace.schema.conversions{direction,format,outcome}` counter through
  a new no-op-default callback, following [#22](../../../AGENTS.md) — no Micrometer types outside
  `observability`, failures swallowed at debug.

---

## 7. Implementation phases

Each phase ends green on `gradle :solace-library:compileJava :client:compileJava :server:compileJava`,
`gradle :client:test :server:test`, and `gradle :solace-library:javadoc`.

### Phase 0 — verify against the jars (half a day)

From a machine that can reach Maven Central:

- `javap` `SerdeHeaders` (constant **values**), `com.solace.serdes.Serializer` / `Deserializer`, the
  exception SERDES throws (and whether registry-down is distinguishable from not-found), `AvroSerializer`'s
  accepted types, `JsonSchemaSerializer`/`Deserializer` with `JsonNode`.
- `javap` `com.solacesystems.jcsmp.serialization.SerdeMessage` in JCSMP 10.30.x; confirm it merges into,
  rather than replaces, an existing `SDTMap`.
- Confirm serializer/deserializer **thread safety**. If not safe, `SerdesFactory` hands out per-thread
  instances.
- `gradle :solace-library:dependencyInsight --dependency sol-jcsmp` after pinning — the direct version
  must beat `solace-spring-boot-bom`. `:client:dependencyInsight --dependency netty` for the Vert.x/Reactor
  Netty clash.
- Add the confirmed rows to the #23 signature table in `AGENTS.md`.

**Exit:** §2 has no unverified row.

### Phase 1 — core prerequisites, no new dependencies (S)

1. `SolaceMessageConverter.toMessage(payload, destination)` default; `SolaceTemplate.createMessage`
   overload; `send(...)` routes through it.
2. Header precedence rule in `DefaultSolaceHeaderMapper` (§4.3).
3. **Fix 3.6:** `solaceListenerContainerFactory` takes `ObjectProvider<SolaceListenerErrorHandler>`.
4. Tests: a recording converter proves both send paths pass the destination; a converter that writes a
   property proves a same-named header does not overwrite it; an error-handler bean reaches the
   container.

Shippable on its own — 3.6 is a bug today regardless of this feature.

### Phase 2 — the converter (M)

`schema` package, `SerdesFactory`, the converter, the exception, the error handler. Unit tests use fake
`Serializer`/`Deserializer` — no registry, no broker, matching what the test suite can run today:

- routing table of §4.4 and §4.5, one case per row;
- `destinations` matching reuses `SolaceTopicMatcher` semantics (`*`, `>`);
- a POJO round-trips through `JsonNode` with the application's naming strategy;
- strict mode rejects a header-less message only on matching destinations;
- each `Reason` maps to its outcome.

### Phase 3 — auto-configuration and properties (S–M)

`SolaceProperties.SchemaRegistry`, `SolaceSchemaRegistryConfiguration`, strategy/profile translation,
startup warnings (`DESTINATION` + request-reply; missing negative-ack; JCSMP too old).
`ApplicationContextRunner` tests for every condition; a binding test in the style of `FlowTuningTest`
proving an untouched block writes no SERDES key.

### Phase 4 — documentation (S)

Per `AGENTS.md`, the public surface change lands in 04, 05 and 15 together:

| Doc | Change |
| :--- | :--- |
| `12-conversion-and-headers.md` | new 12.5 "Schema Registry"; the converter-owns-its-properties rule |
| `05-configuration.md` | `solace.schema-registry.*` |
| `04-spring-integration.md` | the new configuration and its conditions; the error-handler bean fix |
| `15-class-reference.md` | `schema` package table; the new `SolaceMessageConverter` default method |
| `14-extension-points.md` | map row; correct the error-handler row once the fix lands |
| `02-getting-started.md` | dependency and minimal YAML |
| `10-request-reply.md` | map the reply prefix with `>` |
| `17-troubleshooting.md` | wrong-schema reply (3.2), artifact-per-pod (3.4), JCSMP < 10.28, registry outage |
| `18-feature-backlog.md` | move the item to *Recently implemented* with the design notes |
| `AGENTS.md` | constraints #27 (converter owns its properties) and #28 (per-instance topics need wildcard mappings) |

Optional: the metrics callback from §6.

### Phase 5 — demo (M, optional)

- Deploy the registry into the `anupam` namespace from the Helm chart in the Solace distribution (it is
  not in a public chart repo — needs a Solace ID download).
- Put the **inventory** exchange on **JSON Schema**: mappings `request-reply/request-3` →
  `inventory-check` and `request-reply/reply-3/>` → `inventory-status`. It is the one exchange that
  already has its own reply destination, so it avoids the shared-reply problem in §4.6; the quote
  exchange would not. `InventoryCheck`/`InventoryStatus` stay untouched in `shared-dto`; schemas live as
  resources in `client`/`server`.
- Scale the server to 3 and check the registry holds **one** reply artifact, not three — the regression
  test for 3.4.

---

## 8. Effort

| Phase | Size |
| :--- | :--- |
| 0 Verify | XS |
| 1 Core prerequisites | S |
| 2 Converter | M |
| 3 Auto-configuration | S–M |
| 4 Docs | S |
| 5 Demo | M (mostly infrastructure) |

Library work (0–4) is **M**, as the backlog estimated.

---

## 9. Out of scope

| | Why |
| :--- | :--- |
| Protobuf | Solace SERDES 1.0 supports Avro and JSON Schema only |
| Mixed Avro + JSON Schema in one application | Inbound cannot tell the formats apart from the header alone without a registry round trip. v1 is one format per application; a second template/container factory with its own converter covers the rare mixed case |
| `@SolaceListener(schema = …)` | The payload type and topic profile already decide it; an attribute would be a second source of truth |
| Registering schemas from the application | `auto-register` exists for development; production registration belongs in CI or the registry console |
| Kafka-style magic-byte payload framing | Solace carries the id in a header; the payload stays clean |
| Registry health indicator | §6 |

---

## 10. Decisions needed

1. **Format first:** JSON Schema (fits the existing DTOs and the demo with no codegen) or Avro?
   Recommendation: JSON Schema.
2. **Strict inbound default:** `require-schema-id: false` eases migration; `true` is safer once every
   producer is on the registry. Recommendation: `false`, documented.
3. **`use-cached-on-error: true` override** of the SERDES default — agree?
4. **Shared reply destinations under JSON Schema** (§4.6): split the service out, or keep its replies
   ungoverned? Recommendation: split out — it is what the inventory service already does.
5. **Registry access:** is a Solace Schema Registry distribution available (Solace ID) for Phase 5, or
   does the demo stop at unit tests?

---

## Sources

- [Schema Registry overview](https://docs.solace.com/Schema-Registry/schema-registry-overview.htm)
- [SERDES overview](https://docs.solace.com/Schema-Registry/schema-registry-serdes.htm)
- [JCSMP SERDES guide](https://docs.solace.com/API/API-Developer-Guide-JCSMP/JCSMP-API-Serdes.htm)
- [`SerdeMessage` javadoc](https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/serialization/SerdeMessage.html)
- [Avro SERDES javadoc](https://docs.solace.com/API-Developer-Online-Ref-Documentation/java-serdes-avro/allclasses-index.html)
- [JSON Schema SERDES javadoc](https://docs.solace.com/API-Developer-Online-Ref-Documentation/java-serdes-jsonschema/allclasses-index.html)
- [SERDES advanced configuration](https://docs.solace.com/Schema-Registry/schema-registry-serdes-advanced-config.htm)
- [SERDES property reference](https://docs.solace.com/Schema-Registry/schema-registry-serdes-property-reference.htm)
- [JCSMP SERDES tutorial](https://tutorials.solace.dev/jcsmp/schema-registry-serdes/)
- [Deploying the registry with Helm](https://docs.solace.com/Schema-Registry/deploying-schema-registry-helm.htm)

---

**Back to:** [18. Feature backlog](../18-feature-backlog.md)
