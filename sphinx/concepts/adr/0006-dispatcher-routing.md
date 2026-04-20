# ADR-0006: Dispatcher Routing via Named Per-Dispatcher Queues

## Status

Accepted

## Context

Before this ADR, every `JobBuilder` published emitted jobs to a single
hardcoded `JOB_READY_QUEUE` (`constants.QueueName.JOB_READY`). Every
`Dispatcher` consumed from that same queue. AMQP competing-consumer
semantics meant jobs were stolen by whichever dispatcher happened to
win — there was no way for a builder to direct a specific job at a
specific executor.

This broke two realistic deployments:

1. **Mixed-cost workloads.** A service that runs both a cheap
   `serial_bash` workflow and an expensive `slurm_dispatcher` cannot
   send the SLURM-eligible jobs to the right executor.
1. **Fan-out / audit traffic.** A deployment that wants every job
   mirrored to an audit dispatcher alongside its primary executor had
   no mechanism to do so.

We also needed routing to work identically on:

- **Laptop** — memory transport, single process, zero configuration.
- **HA cluster** — RabbitMQ, N replicas of each dispatcher competing
  for its own queue, crash-safe.

## Decision

Route jobs by **selecting the queue at publish time**. Each dispatcher
consumes from a queue named `JobReady-<identifier>` (namespaced to
`<namespace>-JobReady-<identifier>` by `MessageBrokerManager`).
Builders declare a `targets: list[str]` of dispatcher identifiers in
YAML; the builder's `emit()` fans the job out to each target queue.

### Key properties

- **Routing is queue selection**, not a field on `Job` used for
  dispatch. Keeps `Job` narrow and makes routing visible in broker
  introspection.
- **Target identity = `MicroserviceModel.identifier`** — no new
  identity concept.
- **Per-target idempotency** — the emit-claim key is
  `f"{job.identifier}::{target}"`. A crash between two fan-out targets
  leaves completed targets claimed and incomplete ones free for
  resume; neither duplicates nor silently loses deliveries.
- **Publisher confirms on AMQP** — `Service.emit` accepts
  `confirm: bool`; the kombu broker enables publisher confirms on
  non-memory transports. Transient failures (`TransientBrokerError`)
  are retried with backoff; fatal failures
  (`FatalBrokerError`) release the emit claim and log an ERROR so
  restart (or a human) can retake them.
- **Preflight is load-bearing.** Unknown targets, invalid identifiers,
  oversized queue names, and duplicate targets all fail before any
  thread starts.
- **Producer-side queue predeclaration.** Preflight registers every
  dispatcher queue on the broker manager, so the first `emit()` cannot
  race a dispatcher starting after the builder.
- **Consumer-side LRU dedupe.** Each dispatcher replica maintains a
  bounded `OrderedDict` of recent job identifiers and skips duplicates
  (e.g. redelivery after a broker-level nack). Cross-replica strict
  exactly-once is opt-in via the sync-backed dedupe path.
- **Zero-config laptop.** `allow_implicit_target: true` (default)
  auto-wires a single builder to a single dispatcher and emits a
  startup WARNING so silent auto-wiring is always visible. Two or
  more dispatchers requires explicit `targets`.

### Identifier safety

`validate_dispatcher_identifier(ident)` in `constants.py` enforces
`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$` — matches Kubernetes resource
names so operator muscle memory carries over. Preflight also rejects
`<namespace>-<queue>` names exceeding RabbitMQ's 255-character limit
(`MAX_QUEUE_NAME_LENGTH`).

### `TargetResolver` adapter layer

A thin `TargetResolver` (`courier.routing`) sits between
builder-declared target identifiers and the physical queue name used
for publish. The default `IdentityTargetResolver` returns
`job_ready_queue_for(identifier)`. Constructed once at service
startup from the full validated config, injected into every builder,
and reused by `courier queues prune` so the CLI and runtime agree on
the mapping.

Even without a second concrete implementation, the indirection keeps
`Job.targets` as a stable operator-facing label and avoids rethreading
every builder later if operators need multi-cluster routing, aliasing,
or shadow traffic.

## Alternatives considered

- **Label/selector matching (Kubernetes-style).** Rejected per
  `design-principles.md` — speculative abstraction with only one
  concrete use case today. Revisit if a second genuine need surfaces.
- **Broker-native topic exchange with routing-key patterns.** Works
  on RabbitMQ, not on the memory transport. Breaks the laptop story.
- **Job-carried `dispatcher` field + single shared queue + client-side
  filtering.** Breaks competing-consumer HA (every replica sees every
  message) and wastes network.

### External comparison

- **Celery** routes via exchange + routing key.
- **Airflow** uses executor classes bound to queue names per task
  (`queue=` task attribute).

Our design is closest to Airflow's `queue=` attribute — the queue name
IS the target identity. Celery-style topic routing is the natural next
step if label/selector demand materializes.

## Consequences

### Accepted tradeoffs

- **Delivery** — at-least-once per target. Strict exactly-once
  requires opting into the sync-backed dedupe.
- **Ordering** — no cross-target ordering. Within a single target
  queue, ordering is whatever the broker provides (RabbitMQ FIFO per
  queue with one consumer, non-deterministic with multiple competing
  consumers).
- **Idempotency** — jobs should be idempotent. Consumer-side LRU
  dedupe catches same-replica duplicates; sync-backed dedupe (opt-in)
  catches cross-replica.
- **Queue proliferation** — M dispatchers means M queues. Deployments
  with >100 dispatchers should review broker limits.
- **Retired queue cleanup** — removing a dispatcher from config does
  NOT delete its queue from RabbitMQ. Operators run
  `courier queues prune --config PATH` (same `ServiceConfigModel` +
  `TargetResolver` the runtime uses; dry-run default, `--apply`
  required to delete).

### Observability

- **Metrics:** `courier_job_builder_jobs_emitted_total{target}`,
  `courier_job_builder_emit_failures_total{target,reason}`,
  `courier_dispatcher_jobs_consumed_total{dispatcher_identifier}`,
  `courier_dispatcher_dispatch_latency_seconds{dispatcher_identifier}`,
  `courier_dispatcher_queue_depth{dispatcher_identifier}`,
  `courier_dispatcher_dedupe_skips_total{dispatcher_identifier}`.
- **Logs:** INFO on emit (succeeded targets + `correlation_id`);
  single ERROR on partial fan-out listing succeeded and failed targets;
  WARNING on implicit-target auto-wire.
- **Health checks:** dispatcher `is_healthy()` fails if its own
  queue is undeclarable; `preflight_check` fails if any target queue
  is undeclarable.
