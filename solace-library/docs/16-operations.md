# 16. Operations

Running the library in production: what it logs, what to watch, and how to size it.

---

## 16.1 Logging

All library logging is SLF4J under `cris.prs.messaging.solace`.

```yaml
logging:
  level:
    cris.prs.messaging.solace: INFO      # lifecycle and provisioning
    com.solacesystems.jcsmp: WARN        # very chatty at INFO
```

### The lines worth recognising

| Level | Message | Meaning |
| :--- | :--- | :--- |
| INFO | `Solace instance id resolved to '…'` | Once at startup. **The first thing to check** when per-instance destinations look wrong. |
| INFO | `Connected a Solace JCSMP session` | Once per session created. More than a couple means transactional containers each opened their own — expected. |
| INFO | `Provisioned queue '…' for container '…'` | The endpoint did not exist and was created. |
| DEBUG | `The queue '…' already exists` | Normal on every restart. |
| WARN | `The queue '…' already exists with different properties…` | **Your YAML endpoint settings are not in effect.** See 16.4. |
| INFO | `Started Solace listener container '…' [pattern=…, mode=…, endpoint=…, topics=…, concurrency=…, transactional=…, dispatch=…]` | The single most useful line in the log: everything a container resolved to. |
| INFO | `ReplyingSolaceTemplate started, replies expected on '…'` | The reply destination this instance owns. |
| WARN | `Container '…' binds N flows to the exclusive endpoint '…'` | Concurrency exceeds what the endpoint admits. |
| WARN | `Container '…' asks for N flows on a non-durable queue…` | Clamped to 1. |
| WARN | `Received a reply with no outstanding request, correlationId=…` | A reply arrived after its timeout, or for a request this instance never sent. |
| WARN | `Listener returned a value but the request carries no replyTo…` | A responder is returning a value nobody asked for. |
| ERROR | `Solace consumer error in container '…'` | A JCSMP-level flow error. |

### Turning up detail

```yaml
logging:
  level:
    cris.prs.messaging.solace: DEBUG               # registration, provisioning detail
    cris.prs.messaging.solace.requestreply: TRACE  # every request: correlationId, destinations
    org.springframework.transaction: DEBUG         # transaction boundaries
```

`Creating new transaction with name [null]` at transaction DEBUG is normal for a container-driven
transaction — the container uses a `TransactionTemplate`, which sets no name.

---

## 16.2 Micrometer metrics

Meters are published automatically when a `MeterRegistry` bean is present — which
`spring-boot-starter-actuator` provides — and `solace.metrics.enabled` is not `false`. There is
nothing to declare.

```yaml
solace:
  metrics:
    enabled: true          # default
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
```

### Listener meters

All tagged `listener` with the container id.

| Meter | Type | Tags | Meaning |
| :--- | :--- | :--- | :--- |
| `solace.listener.messages.received` | counter | `listener` | Deliveries into the container, counted **before** the listener runs |
| `solace.listener.processing` | timer | `listener`, `result`, `exception` | Time in the listener method; `result` is `success` or `failure`, `exception` is the simple class name or `none` |
| `solace.listener.running` | gauge | `listener` | `1` while the container is running, `0` otherwise |
| `solace.listener.flows` | gauge | `listener` | Flows currently bound |

`received` and the timer's count are separate on purpose. Under `dispatch: EXECUTOR` a message can sit
in the hand-off queue for some time between the two, so the difference is the depth of that buffer —
the clearest signal that a listener is falling behind. Under `INLINE` they track each other closely.

`solace.listener.flows` below the configured concurrency on a running container means flows were lost
without the container stopping.

### Request-reply meters

All tagged `template` with the template id (`solaceReplyContainer` for the auto-configured one, or
whatever `ReplyEndpointSpec.id` you gave an additional destination), and where a request destination
is known, `destination`.

| Meter | Type | Tags | Meaning |
| :--- | :--- | :--- | :--- |
| `solace.requests.sent` | counter | `template`, `destination` | Requests published |
| `solace.requests.send.failed` | counter | `template`, `destination` | Requests that could not be published |
| `solace.requests.latency` | timer | `template`, `destination` | Round-trip time **measured by the requester** |
| `solace.requests.timeouts` | counter | `template`, `destination` | Requests whose reply did not arrive in time |
| `solace.requests.pending` | gauge | `template` | Requests still awaiting a reply |
| `solace.replies.unmatched` | counter | `template` | Replies with no outstanding request |

`solace.requests.pending` is the single most useful number on the requesting side. Unbounded growth
means replies are not being matched — a wrong reply destination, a responder returning `void`, or
timeouts firing faster than replies arrive.

`solace.replies.unmatched` rising alongside `solace.requests.timeouts` after a load spike means the
timeout is too tight. Rising steadily at low volume instead suggests two instances sharing one reply
destination.

### Cardinality

Tag values are bounded by things you control: container ids, template ids, request destinations, and
exception types. Two things to watch:

- a listener that throws many distinct exception types multiplies its timer series;
- request destinations built from unbounded values (an id per request) would produce unbounded series.
  That is a property of the destination naming, not of the instrumentation.

### Turning it off

```yaml
solace:
  metrics:
    enabled: false
```

Containers and templates fall back to their no-op collaborators, so there is no measurement overhead
at all — not merely meters nobody scrapes.

### Adding your own

The SPIs are public. Declare a bean of `SolaceListenerMetrics` or `SolaceRequestReplyMetrics` and it
replaces the Micrometer implementation entirely; both are `@ConditionalOnMissingBean`. Every method
has a no-op default, so implement only what you need:

```java
@Bean
SolaceListenerMetrics solaceListenerMetrics(Tracer tracer) {
    return new SolaceListenerMetrics() {
        @Override
        public void recordFailure(String listenerId, long durationNanos, Exception exception) {
            tracer.currentSpan().error(exception);
        }
    };
}
```

Implementations run on the message path, so they must be cheap. Both call sites are guarded — an
exception is logged at debug and swallowed rather than failing the message — but an implementation
that throws on every message will fill the log.

---

## 16.3 Actuator health

`SolaceHealthIndicator` contributes `/actuator/health/solace` when Actuator is on the classpath and
`solace.health.enabled` is not `false`. It reads state already held in memory and **never contacts the
broker**, so it is cheap enough for a readiness probe on a short interval.

```json
{
  "status": "UP",
  "details": {
    "session": "connected",
    "containers": {
      "orders":  "running (orders.workers)",
      "pricing": "running (pricing.v1)"
    },
    "pendingRequests": { "solaceReplyContainer": 3 }
  }
}
```

### What makes it DOWN

- the session factory reports its connection is gone; or
- a registered listener container is not running, and
  `solace.health.require-all-containers-running` is `true` (the default).

When containers are down, the reason is the first thing in the details:

```json
{
  "status": "DOWN",
  "details": {
    "session": "connected",
    "containers": { "orders": "stopped", "pricing": "running (pricing.v1)" },
    "stoppedContainers": ["orders"]
  }
}
```

### Relax it for manually-controlled containers

```yaml
solace:
  health:
    require-all-containers-running: false
```

A listener declared with `autoStartup = "false"`, or stopped deliberately through the registry, is not
a fault. Left at the default, such a container keeps the instance out of the load balancer
indefinitely.

### Wire it to Kubernetes

Include it in the readiness group so the pod leaves the load balancer when Solace is unusable, and
keep it **out** of liveness so a broker outage does not restart-loop every pod:

```yaml
management:
  endpoint:
    health:
      show-details: always
      group:
        readiness:
          include: readinessState,solace
```

```yaml
readinessProbe:
  httpGet: { path: /actuator/health/readiness, port: 9090 }
  periodSeconds: 5
livenessProbe:
  httpGet: { path: /actuator/health/liveness, port: 9090 }
```

### Known limitation

JCSMP reconnects transparently, and the library does not yet subscribe to session events, so a session
in the middle of a reconnect still reports `connected`. Flow event handling would fix this — see
[18. Feature backlog](18-feature-backlog.md).

### Replacing it

The bean is `@ConditionalOnMissingBean(name = "solaceHealthIndicator")`, so declaring your own bean of
that name replaces it.

---

## 16.4 What else to watch

The broker holds the numbers the application cannot see, and they are usually the ones that matter
first:

| Metric | Watch for |
| :--- | :--- |
| Queue depth per endpoint | Growth = consumers are behind |
| Bind count per endpoint | Should equal `concurrency` × instances |
| Spool usage vs `quota-mb` | A full endpoint rejects publishes |
| Redelivered count | Failures being retried |
| DMQ depth | **Any non-zero value deserves attention** — these are messages you have given up on |
| Client connection count | A leak shows here first |

Alerts worth having, in rough order of value: DMQ depth > 0; `solace.requests.pending` above a
threshold; `solace.listener.running` == 0 for a container that should be up; queue depth growing over
a sustained window; `solace.requests.timeouts` rate above baseline.

---

## 16.5 Sizing

**`concurrency`** is flows per container per instance. Total consumers on a shared endpoint is
`concurrency × replicas`. Start at 1 and raise it only when queue depth grows under load; more flows
on an endpoint that is not backed up just adds context switching.

Ceilings to respect:

- a non-durable queue takes exactly 1;
- an exclusive endpoint takes exactly 1 active consumer;
- a transactional container is capped by `max-transacted-sessions-per-connection`.

**`dispatch-queue-capacity`** (EXECUTOR only) is the per-flow buffer, default 256. Raise it to absorb
bursts, lower it to fail faster under sustained overload. It is deliberately bounded — `put` blocks,
so back-pressure reaches the broker instead of the heap.

**`reply-timeout`** should be comfortably above the responder's p99, and *below* whatever timeout the
caller upstream is enforcing. Too low turns slow responses into orphaned replies, which show up as
`Received a reply with no outstanding request`.

**`max-redelivery-count`** — 3 to 5 is a reasonable default with a DMQ provisioned. Remember `0`
means *forever*, not *never*.

**Connections** — one shared, plus one per transactional container. Multiply by replicas and check it
against the broker's client limit.

---

## 16.6 Endpoint settings and the broker

**The broker never reconfigures an existing endpoint.** `max-redelivery-count`, `quota-mb`,
`access-type`, `permission` and `respects-ttl` are applied only when a queue is first created.
Changing them in YAML on an existing queue does nothing except produce:

```
WARN  The queue 'orders.workers' already exists with different properties, and the broker keeps the
      ones it has. Endpoint settings such as max-redelivery-count and quota are only applied when the
      endpoint is first created: delete it on the broker, or change it through the admin UI or SEMP…
```

To actually change one: alter it via SEMP or the admin UI, or delete the queue and let the
application reprovision it — noting that deleting a queue discards the messages on it.

In environments where an operations team owns the endpoints, turn provisioning off entirely:

```yaml
solace:
  listener:
    provision-endpoint: false
    endpoint:
      dead-message-queue:
        provision: false
```

The client then needs no provision rights, and the application fails fast at startup if an endpoint
it needs is missing.

---

## 16.7 Deployment

**Startup order.** The library needs a reachable broker at container start. JCSMP retries the initial
connection, but a container that cannot bind fails the context. Kubernetes readiness probes should
not report ready until the context has refreshed.

**Graceful shutdown.** Give the pod a grace period longer than `solace.listener.shutdown-timeout`
(default 10s), so `EXECUTOR` invokers can drain rather than being interrupted.

**Rolling restarts.** Temporary endpoints (pub/sub, reply destinations) disappear with the pod and
are recreated by its replacement, under a new name. Durable endpoints keep their messages, so
point-to-point work is simply picked up again.

**Keep-alive.** A listener-only application needs `solace.listener.keep-alive: true` (the default) or
the JVM exits immediately — every JCSMP thread is a daemon thread. Turn it off for an application
that already holds a non-daemon thread, such as a WebFlux or MVC service.

---

## 16.8 A pre-flight checklist

- [ ] `solace.java.host`, `msg-vpn` and credentials are set for the environment
- [ ] `solace.instance-id` resolves to something unique per pod (check the startup log line)
- [ ] Every request-reply service has **its own request topic**, not just its own queue
- [ ] `max-redelivery-count` is non-zero **and** a DMQ is provisioned, or poison messages will loop
- [ ] `concurrency` is within the endpoint's and the transacted-session budget's limits
- [ ] `keep-alive` matches the application type
- [ ] `reply-timeout` is below the caller's timeout and above the responder's p99
- [ ] DMQ depth is alerted on
- [ ] Endpoint provisioning rights match `provision-endpoint`
- [ ] The `solace` health indicator is in the **readiness** group, not liveness
- [ ] `require-all-containers-running` matches whether any listener has `autoStartup = "false"`

---

**Next:** [17. Troubleshooting](17-troubleshooting.md)
