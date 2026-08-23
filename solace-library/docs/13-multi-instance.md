# 13. Multi-instance and destination naming

Running more than one copy of an application is the normal case, and it is where messaging
abstractions usually leak. This page covers the two `support/` classes that keep destination names
correct across instances, and the rules that follow from them.

---

## 13.1 `InstanceIdProvider`

```java
public interface InstanceIdProvider {
    String getInstanceId();
}
```

One method, one value, resolved once at startup. Everything per-instance in the library derives from
it.

## 13.2 `HostnameInstanceIdProvider`

The default. Resolution order:

```
1.  the explicit override      (solace.instance-id)
2.  $HOSTNAME                  ← the pod name in Kubernetes
3.  $POD_NAME                  ← the Downward API convention
4.  InetAddress.getLocalHost().getHostName()
5.  "unknown-" + 8 random hex characters
```

The chosen value is then **sanitised**: `/`, `*`, `>` and any whitespace become `-`. Those are Solace
topic-syntax metacharacters — an unsanitised value containing `/` would silently add a topic level,
and one containing `>` would turn a subscription into a wildcard.

A Kubernetes pod name (`orders-api-7d9f8c-x2k4l`) is already safe and passes through unchanged. A
developer laptop hostname with a space in it does not, and would otherwise produce an invalid
destination.

`HostnameInstanceIdProvider.sanitize(String)` is public and static, so anything building a
destination name can apply the same rule.

The resolved id is logged once at startup:

```
Solace instance id resolved to 'orders-api-7d9f8c-x2k4l'
```

That line is the first thing to check when per-instance destinations look wrong.

### Replacing it

```java
@Bean
InstanceIdProvider solaceInstanceIdProvider(
        @Value("${spring.application.name}") String app,
        @Value("${HOSTNAME:local}") String host) {
    return () -> HostnameInstanceIdProvider.sanitize(app + "-" + host);
}
```

Worth doing when several applications share a Message VPN and you want the destination names to say
which is which.

---

## 13.3 `ReplyDestinationResolver`

A static helper with two rules, used by `ReplyingSolaceTemplateFactory` and available to anything
building the same names.

```java
String resolveTopic(String prefix, boolean appendInstanceId, String instanceId);
String resolveQueueBaseName(String configuredQueue, String topicPrefix);
```

**`resolveTopic`** trims a trailing `/` from the prefix and appends the sanitised instance id when
asked:

| prefix | appendInstanceId | instanceId | Result |
| :--- | :--- | :--- | :--- |
| `reply` | true | `pod-a` | `reply/pod-a` |
| `reply/` | true | `pod-a` | `reply/pod-a` |
| `reply/orders` | true | `pod-a` | `reply/orders/pod-a` |
| `reply` | false | `pod-a` | `reply` |

**`resolveQueueBaseName`** returns the configured queue if there is one, otherwise derives it from
the topic prefix by replacing `/` with `.`:

| configuredQueue | topicPrefix | Result |
| :--- | :--- | :--- |
| `my-replies` | *(anything)* | `my-replies` |
| — | `reply` | `reply` |
| — | `request-reply/reply-1` | `request-reply.reply-1` |

Two conventions are at work: **topics use `/`, endpoint names use `.`**. Deriving one from the other
keeps a reply endpoint recognisably paired with its topic without configuring both.

---

## 13.4 The two places an instance id is appended

They do different things and are controlled separately.

### To the **queue** — `appendInstanceIdToQueue`

```
orders.workers      →   orders.workers.pod-a
```

Each instance binds to its **own** endpoint, so each receives its own copy of every matching message.
This is what makes `PUBLISH_SUBSCRIBE` fan out. Set it to false and every instance binds to the same
endpoint, competing for messages — `POINT_TO_POINT`.

**This one attribute is the difference between broadcast and work-sharing.**

### To the **topics** — `appendInstanceIdToTopics`

```
control/drain       →   control/drain/pod-a
```

The subscription gains a level, so a message published to that exact topic reaches one specific
instance. For targeted control-plane operations: drain this pod, dump this pod's state.

```java
@SolaceListener(pattern = "PUBLISH_SUBSCRIBE", queue = "control", topics = "control/drain",
        appendInstanceIdToTopics = "true")
public void onDrain() { … }
```

```java
solace.send("control/drain/" + targetPod, new DrainCommand());
```

---

## 13.5 Which destinations are per-instance

| Destination | Per instance? | Why |
| :--- | :--- | :--- |
| Reply destination (`reply/<id>`) | **yes**, by default | A reply must land on the pod holding the waiting future |
| `PUBLISH_SUBSCRIBE` endpoint | **yes** | Each instance needs its own copy |
| `POINT_TO_POINT` endpoint | no | The whole point is to share one queue |
| `REQUEST_REPLY` request endpoint | no | Requests are load-balanced across responders |
| DMQ | no | One per Message VPN |

---

## 13.6 Kubernetes

Nothing is required — `HOSTNAME` is the pod name in every container image. Being explicit is still
worth it for readability:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

```yaml
solace:
  instance-id: ${POD_NAME:${HOSTNAME:}}
  listener:
    keep-alive: false          # a WebFlux/MVC app already has a non-daemon thread
```

### Scaling

- **`PUBLISH_SUBSCRIBE`** — scaling out creates one temporary queue per pod. Message volume to the
  broker multiplies by the replica count; the queues vanish when pods do.
- **`POINT_TO_POINT`** — scaling out adds flows to the same durable queue. Throughput rises,
  delivery does not duplicate.
- **`REQUEST_REPLY`** — scaling the *responder* adds flows to the shared request endpoint. Scaling
  the *requester* creates one more reply destination.

### Rolling restarts

Temporary queues (pub/sub endpoints, reply destinations) are deleted when the pod disconnects, so a
rolling restart leaves nothing behind. That also means a message published to a pub/sub topic while a
pod is between restarts is **not** held for it. If a restarting instance must catch up, use a durable
per-group queue instead — see [7.4](07-exchange-patterns.md#74-point_to_point--exactly-one-consumer).

In-flight requests on a terminating pod are failed by `ReplyingSolaceTemplate.stop()` with
`SolaceReplyTimeoutException` rather than being left to hang. Give the pod a `terminationGracePeriod`
long enough for the containers to stop cleanly.

### Verifying

```bash
kubectl logs -l app=orders-api | grep "instance id resolved"
```

```
Solace instance id resolved to 'orders-api-7d9f8c-x2k4l'
Solace instance id resolved to 'orders-api-7d9f8c-p8m2n'
```

Two pods, two ids, two reply destinations. If both show the same id, the per-instance destinations
are colliding and replies will go to the wrong pod.

---

## 13.7 Session events

The session is the TCP connection to the broker, and everything else rides on it. JCSMP reconnects it
transparently, which is convenient and also means a network blip that stops **all** traffic for
several seconds leaves no trace: flows that survive the reconnect raise no flow event, no message is
lost, and nothing in the application notices.

`SolaceSessionEvent` makes that layer visible.

| Event | Meaning |
| :--- | :--- |
| `RECONNECTING` | The connection dropped and JCSMP is retrying. **Nothing is being sent or received** |
| `RECONNECTED` | Back, traffic resumed |
| `DOWN` | Failed unrecoverably; JCSMP has stopped retrying. A restart is required |
| `SUBSCRIPTION_ERROR` | The broker rejected a session subscription — direct consumers lose messages silently |
| `VIRTUAL_ROUTER_NAME_CHANGED` | Reconnected to a **different** broker in an HA pair |
| `INCOMPLETE_LARGE_MESSAGE` | A large message arrived truncated |
| `UNKNOWN_TRANSACTED_SESSION` | The broker does not recognise a transacted session this client believes it has |
| `UNKNOWN` | An event this library does not model |

Logging is unconditional — `DOWN` as error, `RECONNECTING` and `VIRTUAL_ROUTER_NAME_CHANGED` as
warnings. To act on one, declare a bean:

```java
@Bean
SolaceSessionListener solaceSessionListener(Cache cache, AlertService alerts) {
    return args -> {
        switch (args.getEvent()) {
            case VIRTUAL_ROUTER_NAME_CHANGED -> cache.invalidateAll();
            case DOWN -> alerts.page("Solace session down", args.getException());
            default -> { }
        }
    };
}
```

### `VIRTUAL_ROUTER_NAME_CHANGED` is the one that bites

It means the session came back on the *other* broker of an HA pair. Two things did not come with it:

- **temporary endpoints** — every `PUBLISH_SUBSCRIBE` queue and every reply destination in this
  application, which are per-client and per-broker;
- **unacknowledged guaranteed messages** that had not been replicated.

Containers rebind their temporary queues automatically, but anything that assumed continuity across
the failover — an in-flight request whose reply destination has just been recreated, a cache keyed on
broker state — has to be told. That is what this event is for.

### Session state, and what health reports

The factory tracks a `SolaceSessionState` from these events:

| State | Healthy? | |
| :--- | :--- | :--- |
| `NOT_CONNECTED` | yes | No session created yet. An application that has not needed the broker is not broken |
| `CONNECTED` | yes | Normal |
| `RECONNECTING` | **no** | Retrying; nothing is flowing |
| `DOWN` | **no** | Stopped retrying; restart required |

`SolaceSessionFactory.getSessionState()` is the accessor, and `isHealthy()` derives from it. Both are
`default` methods, so a custom session factory keeps compiling and simply reports `CONNECTED`.

The Actuator health indicator reports all four distinctly, and `solace.session.state` gauges them —
see [16.3](16-operations.md#163-actuator-health).

### Two layers, two questions

| | Answers |
| :--- | :--- |
| **Session** events ([13.7](#137-session-events)) | Is the connection to the broker up? |
| **Flow** events ([9.8](09-consuming-messages.md#98-flow-events)) | Is *this consumer* receiving? |

A session can be perfectly healthy while one container's flow is down — its queue was deleted, say.
A session can be reconnecting while every flow object still looks bound. Health reporting needs both,
which is why the indicator checks the session state *and* every container's `isDegraded()`.

---

**Next:** [14. Extension points](14-extension-points.md)
