# Transactions

[Solace local transactions](https://docs.solace.com/Messaging/Guaranteed-Msg/Transactions.htm) are
driven the ordinary Spring way — `@Transactional` or a `TransactionTemplate` — through
[`SolaceTransactionManager`](#solacetransactionmanager), a `PlatformTransactionManager` backed by a
JCSMP `TransactedSession`.

## What a commit covers

A transacted session's `commit()` **acknowledges every message delivered on that session so far and
releases everything published through it**. That single sentence explains most of the design below.

## Consuming and replying atomically

```yaml
solace:
  listener:
    transactional: true
```

A transactional container creates one `TransactedSession` per flow and binds it to the thread before
invoking the listener. The acknowledgement of the request and the publication of the reply commit as
one unit; if the listener throws, neither happens and the broker redelivers.

```
flow ──▶ bind SolaceResourceHolder ──▶ TransactionTemplate.execute
                                          │  listener runs
                                          │  SolaceTemplate.send joins the transaction
                                          ▼
                                       commit  (ack request + release reply)
```

`SolaceTemplate` joins automatically: it publishes through the transacted producer whenever a Solace
transaction is bound to the calling thread, and through the shared producer otherwise. No
transaction-aware variant of the template is needed.

## Publishing in a transaction

```java
@Transactional
public List<WorkItem> dispatchBatch(List<String> descriptions) {
    return descriptions.stream().map(this::dispatch).toList();   // all released at commit, or none
}
```

or programmatically:

```java
transactionTemplate.execute(status -> solace.send(topic, payload));
```

or without Spring transaction infrastructure at all:

```java
solace.executeInTransaction(ops -> {
    ops.send("a/topic", first);
    ops.send("a/topic", second);
    return null;
});
```

> **A request published in a transaction only reaches the broker at commit.** A `RequestReplyFuture`
> must therefore be awaited *outside* the transactional method — waiting inside it blocks on a
> request that has not been sent.

## Why `EXECUTOR` dispatch is rejected for transactional containers

Because commit covers every message delivered on the session, buffering messages away from the
delivery thread would let a commit acknowledge messages that have not been processed yet, and a
rollback redeliver ones that have. Transacted flows are driven by the thread their messages arrive
on. Their parallelism comes from running `concurrency` independent flows, each with its own
transacted session.

## Poison messages

Rollback is the right answer to a transient failure and the wrong one for a message that can never
be handled — it redelivers forever. The cure is broker-side:

```yaml
solace:
  listener:
    endpoint:
      max-redelivery-count: 5
      dead-message-queue:
        provision: true
```

Three things must hold for a message to reach the dead message queue, and the library covers each:
the message was published DMQ-eligible (`solace.template.dmq-eligible`, default `true`); the endpoint
has a `max-redelivery-count`; and the DMQ exists.

## Class reference

### SolaceTransactionManager

`AbstractPlatformTransactionManager` + `ResourceTransactionManager` over a `TransactedSession`.

| Method | Description |
| :--- | :--- |
| `SolaceTransactionManager(SolaceSessionFactory)` | The session factory is also the resource key transactions bind under. |
| `getResourceFactory()` | Returns the session factory, so `TransactionSynchronizationManager` keys resources consistently. |
| `doGetTransaction()` | Picks up a holder already bound to the thread — which is how a listener container's own transacted session becomes the transaction's resource. |
| `isExistingTransaction(Object)` | True only once a holder is marked synchronized, so a container-bound holder still begins a fresh transaction. |
| `doBegin(Object, TransactionDefinition)` | Creates and binds a holder when none exists; marks it synchronized; applies the timeout. |
| `doCommit(DefaultTransactionStatus)` | `TransactedSession.commit()`. |
| `doRollback(DefaultTransactionStatus)` | `TransactedSession.rollback()`. |
| `doSetRollbackOnly(DefaultTransactionStatus)` | Marks the holder rollback-only. |
| `doCleanupAfterCompletion(Object)` | Unbinds and closes **only** a session this manager created. A container's session is reused for the next message. |

### SolaceResourceHolder

Holds the `TransactedSession` bound to the current thread, plus the producer created from it.

| Method | Description |
| :--- | :--- |
| `SolaceResourceHolder(TransactedSession)` | Manager-owned holder; the session is closed after completion. |
| `SolaceResourceHolder(TransactedSession, boolean externallyManaged)` | `externallyManaged = true` for a listener container's session: committed but never closed by the manager. |
| `getProducer()` | The transacted producer, created on first publish. |
| `commit()` / `rollback()` | Wrap the JCSMP calls, translating `RollbackException` and `JCSMPException`. |
| `closeIfOwned()` | Releases the session unless a container owns it. |

### SolaceTransactionUtils

| Method | Description |
| :--- | :--- |
| `getResourceHolder(SolaceSessionFactory)` | The holder bound to this thread, or `null`. |
| `getActiveResourceHolder(SolaceSessionFactory)` | Only a holder whose transaction has actually begun. `SolaceTemplate` uses this to decide whether to join. |
| `bindResourceHolder(...)` / `unbindResourceHolder(...)` | Used by listener containers around each message. |

## Limits

Solace caps transacted sessions per client connection (10 by default). Each transactional container
opens its own connection; the `@Transactional` path shares one, so the limit there applies to
concurrent in-flight transactions. See [Troubleshooting](troubleshooting.md#503-max-transacted-sessions-exceeded).
