# ADR-0010: Poison Message Handling

## Status

Accepted

## Context

`Service._relay` answered every failure the same way:

```python
except GeneratorExit:
    reject()      # msg.reject(requeue=True)
    raise
except Exception:
    reject()
    raise
```

`requeue=True` returns a message to the **head** of its queue. The shipped
`broker_prefetch_count` is 1, so the broker immediately handed the same message
back to the same consumer, which failed on it again, and again. A single
message a consumer could not get past was redelivered forever, and every
message behind it was unreachable. An agent exercising a deliberately broken
ack path measured ~1.6M log lines in under four minutes with zero messages
processed. There was no dead-letter queue, no redelivery cap, and no metric or
log line that said "stuck" — the only symptom was a log file growing.

Two details made this worse than it looks.

**The failure path that matters is not the obvious one.** An exception raised by
a plugin inside the `for` body of a consume loop is *not* thrown into the
generator. Python abandons the generator and the close arrives as
`GeneratorExit`. So the case this guard exists for — a plugin blowing up on one
message — never reached the `except Exception` clause at all. Only a failure
between the `yield` and the `ack` did.

**A counter in the consumer's memory cannot work.** Both plugin interfaces treat
an unhandled exception as fatal and call `os._exit(1)`. An in-process attempt
count would be reset by exactly the crash it is counting, and the message would
be retried forever across container restarts rather than within one process.

Issue #44 had already fixed the opposite failure — an exclusive queue the broker
deleted on disconnect, losing every file published while a builder was away — so
any answer here that discards messages would reintroduce the bug this branch
exists to remove.

## Decision

A message a consumer could not get past is **republished to the tail of its own
queue with an incremented attempt count carried in a header**, and **parked on a
per-queue dead-letter queue** once the count passes `broker_max_redeliveries`
(default 3, `BROKER_MAX_REDELIVERIES`).

- The count lives in the `x-courier-delivery-attempt` header, so it survives the
  process exit that the plugin interfaces perform on error. A message with no
  header — which is every message already queued when this ships — is on its
  first attempt.
- Republishing to the tail is what unblocks the pipeline. It takes effect on the
  *first* failure, before any budget is spent: the backlog moves immediately
  while the failing message waits its turn.
- The republish is confirmed before the original is acknowledged. A crash in
  between duplicates the message rather than losing it, and dispatchers already
  skip a job identifier they have just seen.
- Parking is `<queue>-DeadLetter`, a durable queue courier declares when a
  consumer subscribes. Nothing is discarded; an operator can read the bodies and
  replay them.
- `courier_broker_messages_redelivered_total` and
  `courier_broker_messages_dead_lettered_total` are labelled by queue, with a
  WARNING per retry and an ERROR per park. Any increase in the dead-letter
  counter is a message the service gave up on, and is the thing to alert on.
- Shutdown is exempt. `GeneratorExit` also arrives when a consumer stops with a
  message untouched — the dispatcher's daemon thread is documented as being
  abandoned mid-job if it outlives the join timeout — so `_relay` consults
  `stop_event` and returns those messages unchanged. Counting them would spend
  a retry on every rolling restart and eventually park healthy messages.

## Alternatives Considered

- **`x-dead-letter-exchange` on the existing queues.** The textbook answer, and
  unavailable. A durable queue's arguments are part of what the broker compares
  on redeclaration, so adding one answers 406 PRECONDITION_FAILED for every
  deployment that has already declared `<ns>-FilesFound-<builder>`,
  `<ns>-JobReady-<dispatcher>` or `<ns>-DispatcherQueue` — courier would refuse
  to start until each queue was drained and deleted. `courier queues prune`
  exists and could drive that, but requiring a broker migration to ship a
  bugfix is a bad trade. Decisively, it would not have been sufficient anyway:
  a DLX dead-letters on reject-without-requeue, TTL expiry, or overflow, and
  never on `requeue=True`. It does not count redeliveries, so a cap would still
  have been needed on top of it. Publishing to a queue courier declares itself
  needs no argument on the source queue, so it needs no migration: the only new
  name is one no deployment has declared.

- **`x-delivery-limit`, a broker-side redelivery cap.** Quorum queues only.
  Switching queue type cannot be done in place at all, so this is a strictly
  larger migration than the argument above.

- **Acking and dropping on repeated delivery.** Unblocks the queue and silently
  destroys the file. That is the #44 failure mode with a different cause.

- **Counting redeliveries in the consumer.** Cannot survive `os._exit`; see
  above.

- **Rejecting with `requeue=False` and no dead-letter queue.** Equivalent to
  dropping, since an unbound queue with no DLX discards.

## Trade-offs Accepted

- **Retry ordering is not preserved.** A retried message goes behind whatever is
  queued at the time, so a failing message is reprocessed later than its
  siblings. Courier's consumers are not ordered — job builders bucket by
  identity and dispatchers dedupe — and unblocking the queue is worth more than
  an ordering nothing depends on.

- **Retries are at-least-once, not exactly-once.** A crash between the confirmed
  republish and the ack duplicates a message. Duplicates are recoverable and
  already deduplicated downstream; losses are not.

- **Every consumed queue gains a `-DeadLetter` companion**, declared whether or
  not anything is ever parked. They are added to `courier queues list` and
  preserved by `prune`, since a parked message is the only copy left. Declaring
  eagerly means a name that is too long, or a broker user without `configure`
  permission, fails at startup rather than at the moment a poison message has
  nowhere to go.

- **Abandoning a consume loop mid-message now costs an attempt.** From
  `_relay`'s side an unacknowledged message is indistinguishable from a failed
  one, so a `break` is counted like a raise. No production consumer breaks;
  `Service.consume` documents this and points one-shot callers at a thread with
  a timeout.

- **A builder or dispatcher identifier ending in `-DeadLetter`** would collide
  with another's dead-letter queue. Forbidding it would invalidate configs that
  are legal today, so it is a documented sharp edge rather than a validation
  rule.

## Consequences

- One unprocessable message costs `broker_max_redeliveries + 1` deliveries and
  then stops, instead of blocking its queue indefinitely.

- "Stuck" is now a metric rather than a log-volume anomaly. Alert on
  `increase(courier_broker_messages_dead_lettered_total[1h]) > 0`;
  `courier_broker_messages_redelivered_total` rising on its own is retrying, and
  is expected in small numbers.

- `tests/rabbitmq/test_poison_message_progress.py` pins the behaviour on a real
  broker, which is the only place it exists — the in-memory transport has no
  redelivery semantics, so the same test passes there before and after the fix.
  Four of its five tests fail against the reintroduced bug; the fifth guards the
  shutdown exemption, which is a risk the fix introduces rather than the bug it
  removes, and passes either way.

- Upgrading requires no broker change. Existing messages have no attempt header
  and are treated as first attempts; the dead-letter queues are declared on
  first subscribe.
