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

## 16.2 What to monitor

Nothing is registered with Micrometer automatically. These are the values worth wiring up.

### From the application

| Value | Source | Watch for |
| :--- | :--- | :--- |
| Outstanding requests | `ReplyingSolaceTemplate.getPendingCount()` | Unbounded growth = replies are not being matched |
| Request latency | `RequestReplyFuture.getLatency()` | Per-request round trip, measured by the requester |
| Container running | `registry.getListenerContainers()` → `isRunning()` | A container that stopped and did not restart |
| Resolved endpoint | `container.getResolvedQueueName()` | Not a metric, but the right thing to log and expose |

```java
@Bean
MeterBinder solaceMetrics(ReplyingSolaceTemplate replying, SolaceListenerEndpointRegistry registry) {
    return registry1 -> {
        Gauge.builder("solace.requests.pending", replying, ReplyingSolaceTemplate::getPendingCount)
                .register(registry1);
        registry.getListenerContainers().forEach(container ->
                Gauge.builder("solace.container.running", container,
                                c -> c.isRunning() ? 1 : 0)
                        .tag("listener", container.getListenerId())
                        .register(registry1));
    };
}
```

A health indicator is a few more lines:

```java
@Component
class SolaceHealthIndicator implements HealthIndicator {

    private final SolaceListenerEndpointRegistry registry;

    @Override
    public Health health() {
        List<String> stopped = registry.getListenerContainers().stream()
                .filter(container -> !container.isRunning())
                .map(SolaceMessageListenerContainer::getListenerId)
                .toList();
        return stopped.isEmpty() ? Health.up().build()
                : Health.down().withDetail("stoppedContainers", stopped).build();
    }
}
```

### From the broker

The numbers the application cannot see, and usually the ones that matter first:

| Metric | Watch for |
| :--- | :--- |
| Queue depth per endpoint | Growth = consumers are behind |
| Bind count per endpoint | Should equal `concurrency` × instances |
| Spool usage vs `quota-mb` | A full endpoint rejects publishes |
| Redelivered count | Failures being retried |
| DMQ depth | **Any non-zero value deserves attention** — these are messages you have given up on |
| Client connection count | A leak shows here first |

---

## 16.3 Sizing

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

## 16.4 Endpoint settings and the broker

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

## 16.5 Deployment

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

## 16.6 A pre-flight checklist

- [ ] `solace.java.host`, `msg-vpn` and credentials are set for the environment
- [ ] `solace.instance-id` resolves to something unique per pod (check the startup log line)
- [ ] Every request-reply service has **its own request topic**, not just its own queue
- [ ] `max-redelivery-count` is non-zero **and** a DMQ is provisioned, or poison messages will loop
- [ ] `concurrency` is within the endpoint's and the transacted-session budget's limits
- [ ] `keep-alive` matches the application type
- [ ] `reply-timeout` is below the caller's timeout and above the responder's p99
- [ ] DMQ depth is alerted on
- [ ] Endpoint provisioning rights match `provision-endpoint`

---

**Next:** [17. Troubleshooting](17-troubleshooting.md)
