# ADR-0002: Redis-Based HA State Sync for Job Builders

## Status

Accepted

## Context

When multiple Courier instances run in parallel (high-availability mode), each
instance has its own in-memory `JobGroup` state. Without coordination, the same job
can be emitted to the dispatcher by more than one instance, causing duplicate
processing.

## Decision

Use Redis with `SET NX` (set-if-not-exists) as a lightweight distributed lock. When
`state_sync` is enabled in the job builder config, `JobBuilderStateSync` claims emit
rights for each ready job. Only the instance that wins the `SET NX` race emits the job.

The Redis dependency is optional (`pip install courier[ha]`) and the code path is
entirely disabled when no `state_sync` key is present in the config.

## Alternatives Considered

- **Shared broker deduplication**: Rely on broker-level message deduplication. Not
  reliable with AMQP; requires broker-specific extensions.
- **Single active instance (leader election)**: Zookeeper or etcd for leader election.
  Higher operational complexity; requires a separate coordination service.

## Trade-offs Accepted

- Redis is a runtime dependency for HA deployments, increasing the infrastructure footprint.
- `SET NX` provides at-most-once emission per job ID within the Redis TTL window. Very
  long-running jobs (beyond the TTL) could theoretically be re-emitted.
- State sync is tested in isolation (`tests/unit_tests/sync/`) but integration tests
  with multiple real instances are not yet present.
