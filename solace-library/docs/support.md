# `support` — instance identity and destination naming

Package `cris.prs.messaging.solace.support`. Small, dependency-light helpers that decide how a
horizontally scaled deployment addresses itself.

---

## InstanceIdProvider

Functional interface: `String getInstanceId()`. Supplies the identifier of this application
instance, used to give every pod its own reply destination so that request-reply works across a
scaled deployment.

Replace the bean to source the id from somewhere else — a stateful-set ordinal, a config server, a
lease:

```java
@Bean
InstanceIdProvider solaceInstanceIdProvider() {
    return () -> "region-eu-" + shardId();
}
```

## HostnameInstanceIdProvider

Default implementation. Resolves the id, in order, from:

1. an explicit override (`solace.instance-id`);
2. the `HOSTNAME` environment variable — Kubernetes sets this to the pod name;
3. the `POD_NAME` environment variable;
4. the local host name;
5. a random `instance-xxxxxxxx` suffix, if all else fails.

The result is **sanitised** so it is safe as a single Solace topic level: `/`, `*`, `>` and
whitespace become `-`. Without that, a host name containing a slash would silently split into two
topic levels and route replies to the wrong place.

| Method | Description |
| :--- | :--- |
| `HostnameInstanceIdProvider()` | Resolve with no override. |
| `HostnameInstanceIdProvider(String override)` | Use the override when it has text; resolve otherwise. |
| `getInstanceId()` | The resolved id. Fixed at construction, so it cannot change under a running listener. |
| `sanitize(String)` *(static)* | Make any value safe as one topic level. |

## ReplyDestinationResolver

Static helpers that build the per-instance reply destinations. Appending the instance id as the last
topic level is what lets several replicas share one request topic while each receives only its own
replies.

| Method | Description |
| :--- | :--- |
| `resolveTopic(String prefix, boolean appendInstanceId, String instanceId)` | `app/reply` + `instance-a` → `app/reply/instance-a`. Returns the trimmed prefix when appending is off or the id is empty. |
| `resolveQueueBaseName(String configuredQueue, String topicPrefix)` | The configured queue name when set; otherwise the topic prefix with `/` replaced by `.`, since `/` is not valid in a Solace endpoint name (`app/reply` → `app.reply`). |
