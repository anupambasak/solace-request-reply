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

**Next:** [14. Extension points](14-extension-points.md)
