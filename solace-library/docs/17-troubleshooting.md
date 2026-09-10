# 17. Troubleshooting

Symptom first, then the cause, then the fix. Every entry here is something that has actually
happened.

**Jump to:** [startup](#171-startup-failures) · [binding](#172-binding-and-provisioning) ·
[messages](#173-message-handling) · [request-reply](#174-request-reply) ·
[transactions](#175-transactions)

---

## 17.1 Startup failures

### `No bean named 'solaceListenerContainerFactory' available`

The auto-configuration did not run. Two causes, in order of likelihood:

1. **The application component-scans the auto-configuration package.** A component-scanned
   `@AutoConfiguration` class is treated as an ordinary `@Configuration`, so its conditions are
   evaluated before the Solace starter has contributed `SpringJCSMPFactory`, and every bean is
   skipped. Keep `org.cris.prs.solace.autoconfigure` out of `@ComponentScan`.
2. **`solace.java.host` is not configured**, so the Solace starter contributed nothing and
   `@ConditionalOnClass`/dependency conditions never lead anywhere.

The exception message spells both out. If you are declaring your own auto-configuration class, note
that a class-level `@ConditionalOnBean` on it is order-sensitive and produces exactly this symptom.

### `No qualifying bean of type 'ReplyingSolaceTemplate' … @Qualifier("replyingSolaceTemplate")`

You declared a second `ReplyingSolaceTemplate` (to give one service its own reply destination) and
the auto-configured one backed off.

This was a library bug, fixed by conditioning `replyingSolaceTemplate` on the missing bean **by
name** rather than by type. If you see it, the library predates that fix — or your own bean is
literally named `replyingSolaceTemplate` and is therefore replacing it, which is the documented way
to override.

### `expected single matching bean but found 2: solaceTemplate, replyingSolaceTemplate`

`ReplyingSolaceTemplate extends SolaceTemplate<Object>`, so an unqualified `SolaceTemplate` injection
is ambiguous. `solaceTemplate` is `@Primary`, which normally resolves it — so seeing this means
something removed the `@Primary`, or a third template is in play. Add `@Qualifier("solaceTemplate")`.

### The application starts, logs `Started`, then exits immediately

Every JCSMP thread is a daemon thread. A listener-only application with no web server has no
non-daemon thread and the JVM exits as soon as `main` returns.

**Fix:** `solace.listener.keep-alive: true` (the default). If it is already true, check that at least
one container actually started — `keep-alive` is acquired by containers, so zero containers means
zero references.

### `EXECUTOR dispatch cannot be combined with transactional=true`

Deliberate. A transacted session's `commit()` acknowledges every message delivered on it so far, not
just the one in hand; buffering messages onto another thread would let a commit cover unprocessed
messages and a rollback redeliver processed ones.

**Fix:** use `dispatch: INLINE` for transactional containers. If you set `dispatch: EXECUTOR`
globally in YAML, override it per listener: `@SolaceListener(..., dispatch = "INLINE")`.

### `No converter found capable of converting from type [java.lang.String] to type [...$Flow]`

A nested configuration block whose children are **all commented out**. YAML gives the key an empty
string, not an empty object, and Spring cannot convert that into the properties class:

```yaml
solace:
  listener:
    flow:                            # ✗ nothing under it
      # transport-window-size: 255
```

**Fix:** comment out the parent key as well, or leave one real child. A child with an empty *value*
is fine — that binds as `null`, which is how "unset" is expressed.

The same applies to every nested block: `template`, `listener`, `flow`, `endpoint`,
`dead-message-queue`, `request-reply`, `metrics`, `health`.

### `An AsyncTaskExecutor is required for EXECUTOR dispatch on container '…'`

`solaceListenerTaskExecutor` is missing, usually because a custom container factory was built without
calling `setTaskExecutor`.

---

## 17.2 Binding and provisioning

### `400 Subscription Already Exists`

A durable queue keeps its subscriptions between runs, so from the second start onwards the broker
answers this. It is the expected steady state, not a failure, and the container tolerates it
(`SUBSCRIPTION_ALREADY_PRESENT`).

If it propagates, the subcode is something else — most often the subscription exists with different
properties, which really is a misconfiguration.

### `400 Already Exists` while provisioning

Tolerated the same way (`ENDPOINT_ALREADY_EXISTS`, logged at debug). What you should look for
instead is the **property-mismatch warning**, which means the endpoint exists with settings other
than the ones you configured — and the broker keeps its own. See [16.4](16-operations.md#166-endpoint-settings-and-the-broker).

### `503 Unknown Queue` on a `#P2P/QTMP/…` name

A temporary queue does not exist on the broker until a flow binds to it, so subscribing before
binding fails.

The container orders this correctly — create flows stopped → add subscriptions → start flows. Seeing
this means custom code is provisioning by hand in the wrong order.

### `503 Max clients exceeded for queue`

More flows were bound to an endpoint than it admits. Two distinct causes:

**An exclusive endpoint.** Usually a `PUBLISH_SUBSCRIBE` listener inheriting a global
`solace.listener.concurrency`. The pattern pins `concurrency` to 1 for this reason, and the container
warns whenever an endpoint is asked to bind more than one flow exclusively.

**A non-durable queue, whatever its access type.** `NON_DURABLE_QUEUE` creates a temporary endpoint
(`#P2P/QTMP/…`) owned by the binding client, and a temporary endpoint accepts exactly one flow even
when `NONEXCLUSIVE` was requested. Reply endpoints are non-durable by default, so raising
`concurrency` on one is the common trigger. The container clamps to 1 with a warning; the 503 only
appears from a version without that clamp.

**Fix:** set `concurrency` to 1, or move to a **durable** queue with a non-exclusive access type. For
a reply endpoint that means `replyQueue`, `replyGroup` and `endpointMode: DURABLE_QUEUE` — and *not*
appending the instance id, since the endpoint is then shared. In fan-out, parallelism comes from
running more instances.

### `503 Max Transacted Sessions Exceeded`

A broker allows a fixed number of transacted sessions per client connection, 10 by default. A
transactional container needs one per flow.

Each transactional container opens its own connection, so the budget is per container. What remains
is one container exceeding it on its own: the startup assertion against
`solace.listener.max-transacted-sessions-per-connection` should catch that first, with a clear
message.

**Fix:** raise the broker's client-profile limit **and** the property together, or lower
`concurrency`.

### `May not create additional publisher flows until the default publisher has been created`

JCSMP refuses additional publisher flows on a session until that session's default publisher exists.
The session factory handles this by creating the producer before any transacted session — the rule is
per connection, which is why producers are cached per session.

Seeing it means custom code called `session.createTransactedSession()` directly.

### Consumption stopped but nothing is stopped

Look for `Flow N of container '…' is reconnecting` or `is DOWN` in the log — a container stays
`running` throughout a flow reconnect, so `isRunning()` and the container's lifecycle state both look
fine while nothing is being delivered.

| What you see | What it means |
| :--- | :--- |
| `RECONNECTING`, no `RECONNECTED` after it | JCSMP is still retrying. Raise `solace.listener.flow.reconnect-tries` if brief blips are turning into `DOWN` |
| `DOWN` | **The flow will not recover.** The endpoint was deleted, the bind was rejected, or the error was unrecoverable. The container must be restarted |
| Neither | The flow is fine; look at subscriptions, selectors, or another consumer taking the messages |

The health indicator reports this as `degraded [RECONNECTING]` and goes DOWN, and
`solace.listener.degraded` is the gauge to alert on.

### Every instance thinks it is the leader — or none does

`ACTIVE`/`INACTIVE` events only arrive when **both** hold: the endpoint's access type is `EXCLUSIVE`,
and active flow indication is on. It is derived from the access type, so the usual cause is an
endpoint that is not actually exclusive.

Remember endpoint properties apply only at **first provision** — setting `access-type: EXCLUSIVE` in
YAML does nothing to a queue that already exists as non-exclusive. Check the broker, and check
`solace.listener.active` summed across pods: it should be exactly `1`.

If the endpoint is exclusive but no events arrive, something set
`solace.listener.flow.active-flow-indication: false`.

### Nothing is delivered, but the endpoint exists

Work through, in order:

1. Is the subscription attached? A queue with no subscriptions receives nothing. Check
   `Started Solace listener container … topics=[…]`.
2. Is the publisher using `DeliveryMode.DIRECT` while the consumer binds a queue? Direct messages are
   not spooled to endpoints.
3. Does the topic actually match? `orders/*` matches one level, `orders/>` one or more.
4. Is a `selector` filtering everything out? It is evaluated broker-side, so the message never
   arrives.
5. Is another consumer taking the messages? A shared non-exclusive queue load-balances; check the
   broker's bind count.

---

## 17.3 Message handling

### `MismatchedInputException: No content to map due to end-of-input`

The body was read from the wrong part of the message. A JCSMP dump showing `attLen=43, contentLen=0`
is the giveaway: `BytesMessage.setData()` writes the **binary attachment**, while
`BytesXMLMessage.getBytes()` reads the **XML content part**.

The converter reads the attachment first and falls back to the XML content part. Seeing this means a
custom converter reads only `getBytes()`.

### `could not settle a message as FAILED/REJECTED`

The flow did not negotiate that outcome when it bound. A flow may only send an outcome it asked for
at bind time, and the container derives what to ask for from the configured `error-outcome` — which
cannot see what a `SolaceListenerErrorHandler.resolveOutcome` will decide at runtime.

**Fix:** `solace.listener.negative-acknowledgement: true`.

If it is already true, the broker or client library does not support settlement outcomes: they need
JCSMP 10.17+ and a broker that supports them. Set the property to `false` and use
`ack-on-error`/`max-redelivery-count` instead.

The container logs this and leaves the message for redelivery rather than rethrowing — there is
nothing useful to do with the exception on the JCSMP delivery thread, and the broker resolves the
state anyway.

### A `REJECTED` message vanished instead of reaching the DMQ

`REJECTED` sends a message to the dead message queue only if it is **DMQ-eligible**
(`solace.template.dmq-eligible`, default `true`) and the DMQ exists. An ineligible message, or a
missing `#DEAD_MSG_QUEUE`, means it is discarded.

**Fix:** `solace.listener.endpoint.dead-message-queue.provision: true`, and check the publisher is not
clearing DMQ-eligibility.

### `getDeliveryCount()` returns -1

Delivery counts are a broker feature negotiated per message, and `-1` means it was not available.
Guard every comparison with `isDeliveryCountSupported()` — `-1 > 3` is false, so an unsupported broker
silently looks like a permanent first delivery.

`isRedelivered()` is always available; use it when a boolean is enough.

### A message is redelivered forever

Three settings interact:

| | |
| :--- | :--- |
| `ack-on-error: false` | the message is not acknowledged after a failure |
| `max-redelivery-count: 0` | **redeliver forever**, not "never redeliver" |
| `error-outcome: FAILED` on a message that can never succeed | every attempt fails identically |
| `dead-message-queue.provision: false` | nowhere for an exhausted message to go |

**Fix:**

```yaml
solace:
  listener:
    endpoint:
      max-redelivery-count: 5
      dead-message-queue:
        provision: true
```

For a message that will *never* succeed, `error-outcome: REJECTED` — or a `resolveOutcome` returning
it for that exception type — skips the retries entirely.

Also make sure the messages are DMQ-eligible (`solace.template.dmq-eligible`, default true) — an
ineligible message is *discarded* rather than moved.

Remember `max-redelivery-count` only applies at first provision. On an existing queue you will see
the property-mismatch warning and must change it on the broker.

### A message is acknowledged despite failing

The effective outcome is `ACCEPTED`. Either `error-outcome` is set to it, or — more likely —
`error-outcome` is unset and the deprecated `ack-on-error` defaults to `true`.

**Fix:** `solace.listener.error-outcome: FAILED` to hand the message back for redelivery, counting the
attempt. Prefer that over `ack-on-error: false`, which settles nothing and waits for a rebind.

On a transactional flow `ack-on-error` does not apply — the rollback governs redelivery.

### A listener sees a message twice

Expected under at-least-once delivery. Use `SolaceRecord.isRedelivered()`:

```java
public void onOrder(SolaceRecord<Order> record) {
    if (record.isRedelivered() && alreadyProcessed(record.getCorrelationId())) return;
    process(record.getPayload());
}
```

If it happens *without* the redelivered flag, two consumers are receiving copies — most often two
queues subscribed to the same topic, which is the fan-out arrangement rather than the work-sharing
one.

---

## 17.4 Request-reply

### Replies never arrive

Work through, in order:

1. **Is the responder returning a value?** A `void` method publishes nothing. This is the most common
   cause by a distance.
2. **Is the reply destination reachable?** Compare `ReplyingSolaceTemplate started, replies expected
   on '…'` with what the responder logs, or with the request's `replyTo`.
3. **Are the instance ids unique?** Two pods resolving the same id share a reply destination and
   replies land on the wrong one. Check the `instance id resolved to` lines.
4. **Is `reply-timeout` too short?** Look for `Received a reply with no outstanding request` — that is
   a reply arriving after its future was already failed.
5. **Is the request topic right?** A second service on the *same* request topic as another means both
   answer: one reply matches, the other is an orphan.

### `Received a reply with no outstanding request, correlationId=…`

The reply arrived after its timeout fired, or for a request this instance never sent.

- In volume after a load spike → raise `reply-timeout`.
- Steadily, at low volume → two instances are sharing a reply destination.

### `Listener returned a value but the request carries no replyTo destination`

A responder returned a value for a message published without a `replyTo` — typically a fire-and-forget
publish hitting a `REQUEST_REPLY` listener. The reply is dropped rather than thrown, because the
message itself was handled successfully.

**Fix:** return `void` for one-way messages, or configure `replyDestination` if there genuinely is a
fixed sink.

### `getPendingCount()` grows without bound

Requests are being sent and never completed. Either replies are not arriving (see above), or timeouts
are disabled (`reply-timeout: 0`) and nothing ever fails the futures.

---

## 17.5 Transactions

### `Creating new transaction with name [null]`

Normal, at `org.springframework.transaction: DEBUG`. The name is the `@Transactional` method's
fully-qualified name; a container driving a `TransactionTemplate` directly does not set one.

### A published message survives a rolled-back database transaction

Expected. JCSMP has no XA, so a Solace transaction and a JDBC transaction commit separately. See
[11.5](11-transactions.md#115-interaction-with-a-database) for the three standard remedies —
transactional outbox, publish-after-commit, or idempotent consumers.

### A transactional listener consumes twice the transacted sessions expected

Look for `@Transactional` on the listener method. The flow's transaction is already bound; with
`PROPAGATION_REQUIRES_NEW` the annotation opens a second transacted session per message.

---

## 17.6 Getting more detail

```yaml
logging:
  level:
    org.cris.prs.messaging.solace: DEBUG
    org.cris.prs.messaging.solace.requestreply: TRACE
    com.solacesystems.jcsmp: DEBUG            # very verbose
    org.springframework.transaction: DEBUG
```

Run Spring Boot with `--debug` to print the condition evaluation report, which shows exactly which
Solace beans were created and which conditions were not met.

On the broker side, the queue's **bind count** and **spool depth** answer most "is it the app or the
broker" questions in one look.

---

**Next:** [18. Feature backlog](18-feature-backlog.md)
