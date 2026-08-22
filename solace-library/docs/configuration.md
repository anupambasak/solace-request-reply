# Configuration reference

Every property, its type and its default.

## Broker connection — `solace.java.*`

Owned by `solace-java-spring-boot-starter`, not by this library. It contributes the
`SpringJCSMPFactory` that [`DefaultSolaceSessionFactory`](core.md#defaultsolacesessionfactory)
builds on.

| Property | Default | Description |
| :--- | :--- | :--- |
| `solace.java.host` | — | broker URL, e.g. `tcp://host:55555` |
| `solace.java.msgVpn` | `default` | message VPN |
| `solace.java.clientUsername` | `default` | |
| `solace.java.clientPassword` | | |
| `solace.java.clientName` | generated | client name shown in broker tooling |
| `solace.java.connectRetries` | `0` | `-1` retries forever |
| `solace.java.reconnectRetries` | `0` | `-1` retries forever |
| `solace.java.apiProperties.*` | | passthrough to raw JCSMP properties |

## Instance identity — `solace.*`

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `solace.instance-id` | `String` | `$HOSTNAME` → `$POD_NAME` → local host name → random | Identifies this instance. Becomes the last level of the reply topic and part of per-instance endpoint names. Sanitised so it is safe as one topic level: `/`, `*`, `>` and whitespace become `-`. |

## Template — `solace.template.*`

Applied to the auto-configured `solaceTemplate` and `replyingSolaceTemplate`.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `default-destination` | `String` | `null` | Destination used by `send(payload)` when none is given. Without it that overload throws `IllegalStateException`. |
| `delivery-mode` | `DeliveryMode` | `PERSISTENT` | `PERSISTENT` for guaranteed messaging, `DIRECT` or `NON_PERSISTENT` for at-most-once. |
| `time-to-live` | `long` (ms) | `0` | Message expiry. `0` means never expire. |
| `priority` | `Integer` | `null` | Solace message priority (0–255); unset leaves the broker default. |
| `dmq-eligible` | `boolean` | `true` | Whether an expired or max-redelivered message may be moved to the dead message queue. Required for the DMQ to receive anything. |

## Listener defaults — `solace.listener.*`

Defaults for every `@SolaceListener` container. Bound to
[`ContainerProperties`](listener.md#containerproperties); the annotation overrides them per listener.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `endpoint-mode` | `EndpointMode` | `DURABLE_QUEUE` | `DURABLE_QUEUE`, `NON_DURABLE_QUEUE` or `DIRECT`. See [endpoint modes](#endpoint-modes). |
| `concurrency` | `int` | `1` | Consumer flows bound to the endpoint. Each transactional flow takes one transacted session. |
| `transactional` | `boolean` | `false` | Consume inside a Solace local transaction, so the acknowledgement and any reply commit together. |
| `auto-startup` | `boolean` | `true` | Start with the application context. |
| `provision-endpoint` | `boolean` | `true` | Create the durable endpoint at startup if missing. Never *reconfigures* an existing one. |
| `ack-on-error` | `boolean` | `true` | Acknowledge a message whose listener threw. `false` leaves it unacknowledged for redelivery after rebind. Ignored when transactional — those roll back instead. |
| `keep-alive` | `boolean` | `true` | Hold a non-daemon thread while running, so a consumer-only application stays alive. |
| `dispatch` | `DispatchMode` | `INLINE` | `INLINE` (JCSMP delivery thread) or `EXECUTOR` (Spring `AsyncTaskExecutor`). `EXECUTOR` is rejected when `transactional` is true. |
| `dispatch-queue-capacity` | `int` | `256` | Messages buffered per flow before the delivery thread blocks, under `EXECUTOR` dispatch. Bounded on purpose: it preserves the broker's flow control. |
| `max-transacted-sessions-per-connection` | `int` | `10` | Must match the broker's client profile. A transactional container refuses to start when `concurrency` exceeds it. |
| `shutdown-timeout` | `Duration` | `10s` | How long invokers get to drain buffered messages on stop before being interrupted. |
| `phase` | `int` | `Integer.MAX_VALUE - 100` | `SmartLifecycle` phase. |

### Endpoint properties — `solace.listener.endpoint.*`

Applied when the endpoint is **created**. The broker never reconfigures an existing endpoint on
provision, so changing these does nothing to a queue that already exists.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `access-type` | `AccessType` | `NONEXCLUSIVE` | `EXCLUSIVE` (one consumer) or `NONEXCLUSIVE` (competing consumers). The exchange pattern normally sets this. |
| `permission` | `Permission` | `MODIFY_TOPIC` | `NONE`, `READ_ONLY`, `CONSUME`, `MODIFY_TOPIC`, `DELETE`. |
| `quota-mb` | `int` | `100` | Endpoint spool quota. |
| `respects-ttl` | `boolean` | `true` | Whether the endpoint honours message TTL. |
| `max-redelivery-count` | `int` | `0` | Redeliveries before the message goes to the DMQ. `0` is the broker default: retry forever. Valid range 0–255. |

### Dead message queue — `solace.listener.endpoint.dead-message-queue.*`

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `provision` | `boolean` | `false` | Create the DMQ at startup if missing. |
| `name` | `String` | `#DEAD_MSG_QUEUE` | Solace recognises only this name, one per message VPN. |
| `quota-mb` | `int` | `100` | |
| `access-type` | `AccessType` | `EXCLUSIVE` | |
| `permission` | `Permission` | `CONSUME` | |

`respectsTTL` is forced to `false` for the DMQ: the broker rejects a dead message queue that
respects TTL (`INVALID_PARAMETER_COMBINATION`), since expiry is one of the reasons a message
arrives there.

## Request-reply — `solace.request-reply.*`

Configures the auto-configured [`ReplyingSolaceTemplate`](request-reply.md#replyingsolacetemplate)
and its reply container.

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `boolean` | `true` | Set `false` on services that only consume and never originate requests, so no reply container is created. |
| `reply-topic-prefix` | `String` | `reply` | Base reply topic. Replies arrive on `<prefix>/<instance-id>`. |
| `append-instance-id` | `boolean` | `true` | Append the instance id to the reply topic and endpoint name. Turning this off makes every instance share one reply destination, so replies reach the wrong requester. |
| `endpoint-mode` | `EndpointMode` | `NON_DURABLE_QUEUE` | How the reply endpoint binds. |
| `reply-queue` | `String` | derived from the prefix (`/` → `.`) | Base name of the reply endpoint. |
| `reply-group` | `String` | `null` | Optional group segment for a shared durable reply endpoint. |
| `selector` | `String` | `null` | Optional broker-side selector on the reply endpoint. |
| `concurrency` | `int` | `1` | Flows consuming replies. |
| `reply-timeout` | `Duration` | `30s` | How long a request waits before its future fails with `SolaceReplyTimeoutException`. |
| `delivery-mode` | `DeliveryMode` | `PERSISTENT` | Delivery mode for requests. |

## Endpoint modes

| Mode | Endpoint | Guarantee | Cleanup |
| :--- | :--- | :--- | :--- |
| `DURABLE_QUEUE` | provisioned, survives restarts | guaranteed | manual |
| `NON_DURABLE_QUEUE` | temporary, created on flow bind | guaranteed while connected | broker removes on disconnect |
| `DIRECT` | none — plain topic subscription | at-most-once | n/a |

`DIRECT` supports neither acknowledgement nor transactions, and uses its own connection so that one
container's session-level subscriptions do not leak into another's.

## Worked example

```yaml
solace:
  java:
    host: tcp://broker:55555
    msgVpn: default
    clientUsername: app
    clientPassword: secret
    reconnectRetries: -1

  template:
    delivery-mode: PERSISTENT
    dmq-eligible: true

  listener:
    endpoint-mode: DURABLE_QUEUE
    concurrency: 10
    transactional: true
    dispatch: INLINE
    keep-alive: true
    max-transacted-sessions-per-connection: 10
    endpoint:
      access-type: NONEXCLUSIVE
      permission: MODIFY_TOPIC
      quota-mb: 100
      max-redelivery-count: 5
      dead-message-queue:
        provision: true

  request-reply:
    enabled: true
    reply-topic-prefix: app/reply
    append-instance-id: true
    endpoint-mode: NON_DURABLE_QUEUE
    reply-timeout: 30s
```
