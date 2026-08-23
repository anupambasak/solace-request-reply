# 5. Configuration reference

Every property the library binds, its type, its default, and what it actually changes.

All of it is optional. With only `solace.java.host` set, the defaults below produce a working
application.

---

## 5.1 Complete example

```yaml
solace:
  # --- the broker connection: owned by solace-java-spring-boot-starter, not this library ---
  java:
    host: tcp://localhost:55555
    msg-vpn: default
    client-username: default
    client-password: default

  # --- this library ---
  instance-id: ${HOSTNAME:}          # default: HOSTNAME → POD_NAME → local host → random

  template:
    default-destination:             # used by send(payload)
    delivery-mode: PERSISTENT        # PERSISTENT | DIRECT
    time-to-live: 0                  # ms; 0 = never expires
    priority:                        # 0–255, unset = broker default
    dmq-eligible: true

  listener:
    endpoint-mode: DURABLE_QUEUE     # DURABLE_QUEUE | NON_DURABLE_QUEUE | DIRECT
    concurrency: 1
    transactional: false
    auto-startup: true
    provision-endpoint: true
    ack-on-error: true
    dispatch: INLINE                 # INLINE | EXECUTOR
    dispatch-queue-capacity: 256
    keep-alive: true
    max-transacted-sessions-per-connection: 10
    shutdown-timeout: 10s
    phase: 2147483547                # Integer.MAX_VALUE - 100
    endpoint:
      access-type: NONEXCLUSIVE      # EXCLUSIVE | NONEXCLUSIVE
      permission: MODIFY_TOPIC       # NONE | READ_ONLY | CONSUME | MODIFY_TOPIC | DELETE
      quota-mb: 100
      respects-ttl: true
      max-redelivery-count: 0        # 0 = redeliver forever
      dead-message-queue:
        provision: false
        name: "#DEAD_MSG_QUEUE"
        quota-mb: 100
        access-type: EXCLUSIVE
        permission: CONSUME

  metrics:
    enabled: true                    # publish Micrometer meters when a MeterRegistry exists

  health:
    enabled: true                    # contribute /actuator/health/solace
    require-all-containers-running: true

  request-reply:
    enabled: true
    id: solaceReplyContainer
    reply-topic-prefix: reply
    append-instance-id: true
    endpoint-mode: NON_DURABLE_QUEUE
    reply-queue:                     # default: reply-topic-prefix with '/' → '.'
    reply-group:
    selector:
    concurrency: 1
    reply-timeout: 30s
    delivery-mode: PERSISTENT
```

---

## 5.2 `solace.instance-id`

| | |
| :--- | :--- |
| Type | `String` |
| Default | `$HOSTNAME`, then `$POD_NAME`, then the local host name, then `unknown-<8 hex>` |

Identifies this process. It is appended to every per-instance destination — the reply topic, and any
endpoint with `appendInstanceIdToQueue` — so that two instances of the same application never share
a destination that must be private to one of them.

The resolved value is sanitised: `/`, `*`, `>` and whitespace all become `-`, because those
characters are Solace topic-syntax metacharacters. A raw Kubernetes pod name is already safe and
passes through unchanged.

Set it explicitly only to make destination names predictable in a test.

---

## 5.3 `solace.template.*`

Defaults applied to the auto-configured `solaceTemplate`. They are per-template, not per-message —
`SolaceTemplate` exposes setters for all of them, so an extra template bean can differ.

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `default-destination` | `String` | — | The destination used by `send(payload)`. Calling that overload without this set throws. |
| `delivery-mode` | `DeliveryMode` | `PERSISTENT` | `PERSISTENT` spools the message to matching queues and is acknowledged by consumers. `DIRECT` is at-most-once, not spooled, and cannot be consumed by a queue-based listener. |
| `time-to-live` | `long` (ms) | `0` | Message expiry. `0` means never. Only honoured by an endpoint whose `respects-ttl` is true. |
| `priority` | `Integer` | unset | 0–255. Affects broker-side ordering when the endpoint supports it. Leave unset unless you know the endpoint is configured for priority. |
| `dmq-eligible` | `boolean` | `true` | Whether an expired or max-redelivered message moves to the DMQ instead of being discarded. |

---

## 5.4 `solace.listener.*`

Defaults for **every** `@SolaceListener` container. Anything set on the annotation, and anything a
pattern implies, overrides these — see [5.8 Precedence](#58-precedence).

`SolaceProperties.Listener extends ContainerProperties`, so this table is also the reference for a
hand-built `ContainerProperties`.

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `endpoint-mode` | `EndpointMode` | `DURABLE_QUEUE` | See [5.5](#55-endpointmode-in-detail). |
| `concurrency` | `int` | `1` | Number of **flows** bound to the endpoint. Each flow delivers independently, so this is the container's parallelism. Clamped to 1 for `NON_DURABLE_QUEUE`. |
| `transactional` | `boolean` | `false` | Bind each flow to its own `TransactedSession`, so the acknowledgement and anything published in the listener commit together. Forces `INLINE` dispatch. |
| `auto-startup` | `boolean` | `true` | Whether the container starts with the context, or waits to be started through the registry. |
| `provision-endpoint` | `boolean` | `true` | Create the durable queue (and the DMQ) if missing. Set `false` when endpoints are managed by an operations team and the client lacks provision rights. |
| `ack-on-error` | `boolean` | `true` | After the error handler runs on a **non-transactional** flow, acknowledge anyway. `false` leaves the message unacknowledged, so the broker redelivers — combine with `max-redelivery-count` and a DMQ or it will loop. Ignored on a transacted flow, where the rollback governs redelivery. |
| `dispatch` | `DispatchMode` | `INLINE` | `INLINE` runs the listener on the JCSMP delivery thread. `EXECUTOR` hands it to `solaceListenerTaskExecutor`. |
| `dispatch-queue-capacity` | `int` | `256` | Per-flow hand-off queue for `EXECUTOR`. Bounded on purpose: `put` blocks, so a slow listener pushes back on the broker rather than filling the heap. |
| `keep-alive` | `boolean` | `true` | Hold a non-daemon thread while any container runs, so a listener-only application does not exit. Set `false` if the app already has one (a web server). |
| `max-transacted-sessions-per-connection` | `int` | `10` | Must match the broker's client-profile limit. A transactional container refuses to start if its `concurrency` exceeds it. |
| `shutdown-timeout` | `Duration` | `10s` | How long a stopping `EXECUTOR` invoker is given to drain before it is interrupted. |
| `phase` | `int` | `2147483547` | `SmartLifecycle` phase. Late, so messaging starts after the rest of the app and stops before it. |

### `solace.listener.endpoint.*`

Endpoint properties applied when a durable queue is **provisioned**. The broker never reconfigures an
existing endpoint, so these take effect only the first time a queue is created; a mismatch is logged
as a warning.

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `access-type` | `AccessType` | `NONEXCLUSIVE` | `EXCLUSIVE` — one active consumer, the rest standby. `NONEXCLUSIVE` — competing consumers. |
| `permission` | `Permission` | `MODIFY_TOPIC` | What non-owner clients may do: `NONE`, `READ_ONLY`, `CONSUME`, `MODIFY_TOPIC`, `DELETE`. `MODIFY_TOPIC` is needed for a client to add its own subscriptions to the queue. |
| `quota-mb` | `int` | `100` | Spool quota. The broker rejects publishes to a full endpoint. |
| `respects-ttl` | `boolean` | `true` | Whether messages on this endpoint expire per their TTL. |
| `max-redelivery-count` | `int` | `0` | Redelivery attempts before the message is moved to the DMQ (or discarded if not DMQ-eligible). `0` means redeliver indefinitely — a poison message will loop forever. |

### `solace.listener.endpoint.dead-message-queue.*`

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `provision` | `boolean` | `false` | Create the DMQ at container start if missing. Turn on together with `max-redelivery-count`. |
| `name` | `String` | `#DEAD_MSG_QUEUE` | Fixed by Solace convention. A message VPN has exactly one, and the broker only routes to this name. Changing it means the broker will not use it. |
| `quota-mb` | `int` | `100` | Spool quota for the DMQ. |
| `access-type` | `AccessType` | `EXCLUSIVE` | Dead messages are usually inspected by one tool. |
| `permission` | `Permission` | `CONSUME` | |

The DMQ is provisioned with `respects-ttl` forced to `false`: a message that arrived because it
expired must not immediately expire again in the queue meant to preserve it for inspection.

---

## 5.5 `EndpointMode` in detail

| Mode | Broker object | Lifetime | Flows | Delivery | Typical use |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DURABLE_QUEUE` | Provisioned queue | Survives restarts, keeps subscriptions and messages | `concurrency` | Guaranteed, acknowledged, redelivered | Point-to-point work queues, request endpoints |
| `NON_DURABLE_QUEUE` | Temporary queue `#P2P/QTMP/…` | Created on bind, deleted on disconnect | **exactly 1** | Guaranteed while connected | Per-instance fan-out, reply destinations |
| `DIRECT` | none — subscriptions on the session | Connection lifetime | n/a | At-most-once, no ack, no redelivery | High-rate telemetry where loss is acceptable |

Two consequences that cause most first-time confusion:

- A **non-durable queue accepts one flow whatever access type you ask for**. It is owned by the
  binding client. `concurrency: 2` on one is clamped to 1 with a warning; older behaviour was a
  `503 Max clients exceeded for queue`.
- **`DIRECT` mode cannot receive `PERSISTENT` messages spooled to a queue**, because there is no
  queue. Producer and consumer must agree.

---

## 5.6 `solace.request-reply.*`

`SolaceProperties.RequestReply extends ReplyEndpointSpec`, so this table is also the reference for a
hand-built spec passed to `ReplyingSolaceTemplateFactory`. Only `enabled` is specific to the
auto-configured bean.

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | `true` | `false` removes the `replyingSolaceTemplate` bean entirely. Set it on a pure responder, which has no reply destination to consume. |
| `id` | `String` | `solaceReplyContainer` | Container id for the reply container. Must be unique when there is more than one reply destination. |
| `reply-topic-prefix` | `String` | `reply` | Base of the reply topic. The instance id is appended to it. |
| `append-instance-id` | `boolean` | `true` | Makes the reply destination private to this instance. Turn off only for a shared durable reply endpoint, where replies are load-balanced across instances — and then correlation must be handled by whichever instance receives them. |
| `endpoint-mode` | `EndpointMode` | `NON_DURABLE_QUEUE` | Temporary is right for a per-instance reply destination: it disappears with the pod. Use `DURABLE_QUEUE` only for a shared reply endpoint. |
| `reply-queue` | `String` | `reply-topic-prefix` with `/` → `.` | Endpoint name backing the reply topic. |
| `reply-group` | `String` | — | Appended as `<queue>.<group>`, for a shared durable reply endpoint. |
| `selector` | `String` | — | Broker-side filter on the reply endpoint. |
| `concurrency` | `int` | `1` | Flows consuming replies. Only meaningful with `DURABLE_QUEUE` — clamped to 1 otherwise. |
| `reply-timeout` | `Duration` | `30s` | Default wait before a future fails with `SolaceReplyTimeoutException`. Zero or negative waits forever. Overridable per call. |
| `delivery-mode` | `DeliveryMode` | `PERSISTENT` | Delivery mode for **requests** published through this template. |

The resulting reply destination is `<reply-topic-prefix>/<sanitised instance id>`, e.g.
`reply/orders-api-7d9f8c-x2k4l`.

---

## 5.7 `solace.metrics.*` and `solace.health.*`

Both are optional integrations, and both disappear cleanly when their dependency is absent —
Micrometer for metrics, Spring Boot Actuator for health.

| Property | Type | Default | Effect |
| :--- | :--- | :--- | :--- |
| `metrics.enabled` | `boolean` | `true` | Publish Solace meters when a `MeterRegistry` bean exists. `false` leaves containers and templates on their no-op collaborators, so there is **no measurement overhead at all** — not merely meters nobody scrapes. |
| `health.enabled` | `boolean` | `true` | Contribute a `solace` health indicator when Actuator is present. |
| `health.require-all-containers-running` | `boolean` | `true` | Report DOWN when a registered listener container is not running. Set `false` for an application that starts containers by hand or declares listeners with `autoStartup = "false"` — a deliberately idle container is not a fault, and reporting it as one keeps the instance out of the load balancer. |

See [16.2](16-operations.md#162-micrometer-metrics) for the meters and
[16.3](16-operations.md#163-actuator-health) for the health details.

---

## 5.8 Precedence

For a `@SolaceListener` container, from lowest to highest:

```
1.  ContainerProperties defaults (the Java field initialisers)
2.  solace.listener.*                       ← YAML
3.  @SolaceListener attributes              ← anything set explicitly here
4.  ExchangePattern defaults                ← only fills values still unset
```

Step 4 is applied **last** but only writes into fields that are still `null`, which is why an
explicit annotation attribute always wins over a pattern default. Every `SolaceListenerEndpoint`
override field is a boxed type (`Integer`, `Boolean`) precisely so that "unset" is distinguishable
from "set to the default value".

Worked example:

```yaml
solace.listener.concurrency: 10
```
```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "audit", topics = "orders/>")
```
The pattern sets `endpointMode = NON_DURABLE_QUEUE`, `accessType = EXCLUSIVE`,
`appendInstanceIdToQueue = true`, and `concurrency = 1` — the last of these *because the annotation
did not set it*. Without the pattern pinning it, the container would inherit 10 from YAML and try to
bind ten flows to a one-flow temporary endpoint.

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "audit", topics = "orders/>", concurrency = "3")
```
Here `concurrency` was set explicitly, so the pattern leaves it at 3 — and the container's
non-durable clamp brings it back to 1 with a warning at start. Explicit intent is respected as far
as the broker allows, and the discrepancy is reported rather than hidden.

---

## 5.9 Environment-specific patterns

**Local development** — no persistence, no provisioning rights needed:

```yaml
solace:
  listener:
    endpoint-mode: NON_DURABLE_QUEUE
    keep-alive: true
  template:
    delivery-mode: PERSISTENT
```

**Kubernetes** — instance id comes from the pod, keep-alive off for a web app:

```yaml
solace:
  instance-id: ${POD_NAME:${HOSTNAME:}}
  listener:
    keep-alive: false            # WebFlux already holds a non-daemon thread
    endpoint:
      max-redelivery-count: 5
      dead-message-queue:
        provision: true
```

**Pure responder** — nothing to reply *to*, so drop the reply container:

```yaml
solace:
  request-reply:
    enabled: false
```

**Endpoints managed by operations** — the app must not try to create anything:

```yaml
solace:
  listener:
    provision-endpoint: false
    endpoint:
      dead-message-queue:
        provision: false
```

---

**Next:** [6. Annotations](06-annotations.md)
