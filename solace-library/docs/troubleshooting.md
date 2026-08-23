# Troubleshooting

Failures seen in practice, what each one means, and the fix.

---

## `No bean named 'solaceListenerContainerFactory' available`

The auto-configuration did not run, yet the annotation processor did.

Almost always the application component-scans `cris.prs.solace.autoconfigure`. A scanned
`@AutoConfiguration` class is treated as a plain `@Configuration`, so its conditions are evaluated
before `SolaceJavaAutoConfiguration` has contributed `SpringJCSMPFactory`, and every bean is skipped —
while `@EnableSolace` on `SolaceAnnotationDrivenConfiguration`, also scanned, still registers the
post-processor.

**Fix:** keep the autoconfigure package out of your `@ComponentScan` base packages. Also check
`solace.java.host` is set; without a broker connection the starter contributes nothing.

---

## `400 Subscription Already Exists`

A durable endpoint keeps its subscriptions between runs, so on every restart after the first the
broker reports the subscription is already there.

The library treats subcode `SUBSCRIPTION_ALREADY_PRESENT` as the expected steady state and logs at
debug. `SUBSCRIPTION_ATTRIBUTES_CONFLICT` — the same subscription with different properties — still
propagates, because that is a real misconfiguration.

---

## `400 Already Exists` while provisioning

Normal. Provisioning tolerates `ENDPOINT_ALREADY_EXISTS`.

Worth reading the next line, though: on `ENDPOINT_PROPERTY_MISMATCH` the container logs a **warning**
that the broker kept its own settings. **Endpoint properties are applied only when the endpoint is
created** — adding `max-redelivery-count` or changing a quota does nothing to a queue that already
exists. Delete the queue so it is recreated, or change it through the admin UI or SEMP.

---

## `503 Unknown Queue` on a `#P2P/QTMP/...` name

A temporary queue does not exist on the broker until a flow binds to it, so subscribing first fails.

Handled: the container creates flows (stopped) → adds subscriptions → starts flows. If you see this
after customising the container, that ordering is what to restore.

---

## `503 Max clients exceeded for queue`

More flows were bound to an endpoint than it admits. There are two ways to get here.

**An exclusive endpoint.** Usually a publish-subscribe listener inheriting a global
`solace.listener.concurrency`. An exclusive endpoint takes one consumer; `PUBLISH_SUBSCRIBE`
therefore defaults `concurrency` to 1, and the container warns whenever any endpoint is asked to bind
more than one flow exclusively.

**A non-durable queue, whatever its access type.** `NON_DURABLE_QUEUE` creates a temporary endpoint
(`#P2P/QTMP/...`) owned by the binding client, and a temporary endpoint accepts exactly one flow even
when `NONEXCLUSIVE` was requested. Reply endpoints are non-durable by default, so raising
`concurrency` on one is the common trigger. The container clamps `concurrency` to 1 in this mode and
logs a warning; the 503 above only appears from a version without that clamp.

**Fix:** set `concurrency` to 1, or move to a **durable** queue with a non-exclusive access type to
consume in parallel — for a reply endpoint that means setting `replyQueue`, `replyGroup` and
`endpointMode: DURABLE_QUEUE`, and *not* appending the instance id, since the endpoint is then shared
across instances. In fan-out, parallelism comes from running more instances.

---

## `503 Max Transacted Sessions Exceeded`

Solace caps transacted sessions per client connection at 10 by default. A transactional container
takes one per flow, so two containers at concurrency 10 and 5 need 15 between them.

Handled: each transactional container opens its own connection, and refuses to start when its own
`concurrency` exceeds `solace.listener.max-transacted-sessions-per-connection`.

Still reachable from the `@Transactional` path, which shares the session — there the limit applies to
**concurrent in-flight transactions**. Raise `max-transacted-sessions` on the broker's client profile
and the property to match, or lower concurrency.

---

## `May not create additional publisher flows until the default publisher has been created`

JCSMP requires a session's default publisher (`getMessageProducer`) before any other publisher flow,
including a transacted session's producer. A service that only ever publishes inside transactions
never triggers it otherwise.

Handled: `DefaultSolaceSessionFactory.createTransactedSession(session)` creates the default publisher
first, per connection.

---

## `MismatchedInputException: No content to map due to end-of-input`

The message body was read from the wrong part of the message.

`BytesMessage.setData()` writes the **binary attachment**; `BytesXMLMessage.getBytes()` reads the
**XML content part**. A message dump showing `attLen=43, contentLen=0` has its payload in the
attachment. `JacksonSolaceMessageConverter` reads the attachment first and falls back to the XML
content, and reports an empty body explicitly rather than as a parse error.

---

## The application starts, logs `Started`, then exits immediately

Every JCSMP thread is a daemon thread, so a listener-only application with no web server has nothing
holding the JVM open.

Choose one:

* `solace.listener.keep-alive: true` (the default) — a container holds one non-daemon thread;
* `solace.listener.dispatch: EXECUTOR` — invoker threads are non-daemon (not available for
  transactional containers);
* add a web starter and let the servlet or Netty container own the process lifetime.

JCSMP exposes no thread-factory hook, and `Thread.setDaemon` cannot be changed on a running thread,
so making its threads non-daemon is not an option.

---

## `EXECUTOR dispatch cannot be combined with transactional=true`

By design. A transacted session's `commit()` acknowledges every message delivered on it, so buffering
messages off the delivery thread would let a commit cover unprocessed messages and a rollback
redeliver processed ones.

Use `dispatch: INLINE` for transactional containers. Their parallelism already comes from
`concurrency` independent flows.

---

## A message is redelivered forever

Rollback is correct for a transient failure and wrong for a message that can never be handled.

```yaml
solace:
  listener:
    endpoint:
      max-redelivery-count: 5
      dead-message-queue:
        provision: true
```

Requires the message to have been published DMQ-eligible (default `true`) and the endpoint to have
been **created** with the redelivery count — see the property-mismatch note above.

---

## Replies never arrive

* `GET` the value of `ReplyingSolaceTemplate.getReplyDestination()` and confirm the responder is
  publishing there — it should end with this instance's id.
* Check `solace.request-reply.append-instance-id` is `true`. With it off, every instance shares one
  reply destination and replies reach the wrong requester.
* Confirm the responder copies the correlation id. A returned value handled by the container does
  this automatically; a hand-built `Message<?>` must carry `solace_correlationId`.
* `getPendingCount()` growing means requests are published but nothing matches the replies.

---

## `Creating new transaction with name [null]`

Cosmetic. Spring logs `TransactionDefinition.getName()`, which `@Transactional` fills in and a plain
`TransactionTemplate` — which the listener container uses — does not.
