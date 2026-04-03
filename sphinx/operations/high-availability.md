# High-Availability Job Builder State Sync

Lazy Lemon supports running multiple instances of a service simultaneously
for **fault tolerance** and **increased throughput**. When two or more
instances share the same input data stream, their job builders must agree on
which files belong to which job — otherwise one instance can dispatch a
half-complete job while the other dispatches a duplicate.

The **state-sync** feature solves this by connecting every job builder instance
to a shared Redis server. Each instance pushes its mutations to Redis and
subscribes to peer updates so that all instances converge to the same job-group
state in near real-time.

```{note}
State sync is **optional and disabled by default**. A single-instance
deployment does not need Redis and the ``redis`` package is not imported unless
``state_sync`` appears in a job builder's config.
```

## When to Enable State Sync

Enable state sync when you run **more than one Lazy Lemon instance** pointing
at the same data source. Common scenarios:

- Active-active redundancy where either instance can take over if the other
  fails.
- Horizontal scale-out where multiple instances divide a high-volume ingest
  stream across cores or hosts.
- Rolling deployments where an old and a new instance overlap briefly during
  a restart.

A **single-instance** deployment does not benefit from state sync and should
leave it not configured.

## Architecture

### Redis Data Layout

State sync uses three types of Redis keys per job builder, all namespaced by
the configured `channel_prefix`, the service namespace, and the builder name:

| Key pattern                                | Redis type      | Purpose                                                                              |
| ------------------------------------------ | --------------- | ------------------------------------------------------------------------------------ |
| `{prefix}:{ns}:{builder}:{group}:jobs`     | Hash            | Persistent store of all jobs in a group. Field = `job_id`, value = serialized `Job`. |
| `{prefix}:{ns}:{builder}:state_changes`    | Pub/Sub channel | Lightweight notifications (`job_updated` / `job_deleted`) sent after each mutation.  |
| `{prefix}:{ns}:{builder}:{job_id}:claimed` | String (SETNX)  | Exclusive emit guard. Only the instance that creates this key may dispatch the job.  |

### How a Mutation Propagates

When instance **A** receives a new file and updates a job:

1. The file-processing thread acquires the per-group lock.
1. It calls `add_file()` on the local `JobGroup` to update the in-memory state.
1. It writes the updated job to the Redis hash (`HSET`) and publishes a
   `job_updated` notification to the pub/sub channel.
1. The per-group lock is released.

When instance **B** (a peer) receives the notification on its subscriber thread:

1. It acquires the same per-group lock.
1. It fetches the updated job from the Redis hash (`HGET`).
1. It merges the remote job into its local state using
   **last-write-wins** on `Job.last_modified`.

### Conflict Resolution

Instances can race. If both A and B add a file to the same group at the same
instant, both will write to Redis and publish. The merge rule is simple and
deterministic: the job with the **larger `last_modified` timestamp wins**. No
coordination is needed at merge time, and no state is lost — the eventual state
reflects the most recent write across all instances.

### Duplicate-Dispatch Prevention

When a job group signals that a job is ready to emit, every instance that holds
a copy of the job will independently detect the ready condition. Without a
guard, every instance would dispatch the same job.

State sync prevents this with an **atomic Redis SET NX** (set-if-not-exists)
before each `emit()` call:

```
SET {prefix}:{ns}:{builder}:{job_id}:claimed 1 NX EX {ttl}
```

- The instance that wins the SETNX call proceeds to dispatch.
- All other instances skip the emit for that job.
- The claim key expires automatically (TTL = job's estimated processing time),
  so the key does not accumulate indefinitely.

If Redis is temporarily unreachable during the claim attempt, the method
**fails open** — the instance proceeds with the emit rather than silently
dropping the job. This is the safer failure mode because a duplicate dispatch
is recoverable (idempotent downstream processing), whereas a silently lost job
is not.

### Startup State Recovery

When a new or restarted instance starts its job builder, it loads all existing
job state from Redis **before** opening the pub/sub subscription. This means a
restarted instance picks up any in-progress groups without losing accumulated
files. The same last-write-wins merge applies at load time.

## Prerequisites

State sync requires:

- **Redis 4.0+** (pub/sub and `SET NX EX` are used)
- The `lazylemon[ha]` package extra:

```bash
pip install lazylemon[ha]
```

This installs the `redis` Python package. If `state_sync` is configured but
`redis` is not installed, the job builder will raise `InvalidPluginConfigError`
at startup with a clear message.

## Configuration

Add a `state_sync` block to the job builder's `config` section:

```yaml
- build:
    kind: job_builder
    name: filter_and_group
    config:
      state_sync:
        host: redis.internal      # Required. Redis hostname or IP.
        port: 6379                # Default: 6379.
        db: 1                     # Default: 0.  Redis database index.
        password: "${REDIS_PASS}" # Default: null (no auth).
        ssl: false                # Default: false.
        channel_prefix: lazylemon # Default: "lazylemon".
```

### Field Reference

| Field            | Type    | Required | Default       | Description                                |
| ---------------- | ------- | -------- | ------------- | ------------------------------------------ |
| `host`           | string  | No       | `"localhost"` | Redis server hostname or IP.               |
| `port`           | integer | No       | `6379`        | TCP port (1--65535).                       |
| `db`             | integer | No       | `0`           | Redis database index (>= 0).               |
| `password`       | string  | No       | `null`        | AUTH password. `null` or empty skips auth. |
| `ssl`            | boolean | No       | `false`       | Use TLS (`rediss://`).                     |
| `channel_prefix` | string  | No       | `"lazylemon"` | Key prefix for multi-tenant isolation.     |

```{note}
State-sync Redis is **always a separate connection** from the message broker
Redis. Even if your broker also uses Redis, configure them independently so
that a broker restart does not affect state-sync and vice versa.
```

### Multi-Tenant Isolation

If multiple services share one Redis instance, set a unique `channel_prefix`
per service (or per namespace):

```yaml
state_sync:
  host: shared-redis.internal
  channel_prefix: goes18-prod   # Unique per logical service.
```

All Redis keys and pub/sub channels include the prefix, so there is no
cross-service key collision.

## Complete Example

Two-instance active-active deployment watching the same ingest directory:

**`goes18-ha.yaml`** (identical on both hosts):

```yaml
apiVersion: lazylemon.dev/v1alpha1
kind: Service
metadata:
  name: goes18-ha
  namespace: production
  description: GOES-18 two-instance HA pipeline.

spec:
  broker:
    transport: amqp
    host: rabbitmq.prod.internal
    port: 5671
    username: svc_goes18
    password: "${RABBITMQ_PASSWORD}"
    vhost: /geoips
    ssl: true

  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/goes18/incoming

    - build:
        kind: job_builder
        name: filter_and_group
        config:
          state_sync:
            host: redis.prod.internal
            port: 6379
            db: 2
            password: "${REDIS_SYNC_PASSWORD}"
            ssl: true
            channel_prefix: goes18-prod

    - dispatch:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            run_geoips.sh {file}
```

Start both instances (on different hosts or in different containers):

```bash
lazylemon run goes18-ha.yaml
```

Both instances will:

1. Connect to the shared state-sync Redis and fail fast if it is unreachable.
1. Load any pre-existing job state from Redis.
1. Subscribe to peer updates.
1. Process incoming files, pushing every mutation to Redis.
1. Race to claim emit rights when a job becomes ready — only one will win.

## Failsafe Behavior at Startup

State sync uses a **fail-fast** strategy: if the state-sync Redis is
unreachable when the service starts, the job builder refuses to start and
raises `StateSyncConnectionError`. This prevents a silent split-brain where
one instance operates with stale state.

```
StateSyncConnectionError: Cannot connect to state-sync Redis at
  redis.prod.internal:6379 db=2 — Connection refused
```

Fix the Redis connectivity issue and restart the service. Do not disable state
sync in an HA deployment to work around a Redis outage — doing so will cause
duplicate dispatches until Redis is restored.

## Observability

Three Prometheus metrics are exported when state sync is active:

| Metric                                   | Type    | Labels                   | Description                                                                                                 |
| ---------------------------------------- | ------- | ------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `lazylemon_state_sync_pushes_total`      | Counter | `builder_name`, `event`  | Job state pushes sent to Redis. `event` is `job_updated` or `job_deleted`.                                  |
| `lazylemon_state_sync_applies_total`     | Counter | `builder_name`           | Remote job state updates merged into local state.                                                           |
| `lazylemon_state_sync_emit_claims_total` | Counter | `builder_name`, `result` | Emit claim attempts. `result` is `acquired` (this instance dispatches) or `skipped` (peer already claimed). |

### Useful Queries

**Fraction of jobs this instance dispatched** (vs. skipped to peers):

```promql
rate(lazylemon_state_sync_emit_claims_total{result="acquired"}[5m])
/
rate(lazylemon_state_sync_emit_claims_total[5m])
```

**Rate of remote state updates received** (peer synchronization activity):

```promql
rate(lazylemon_state_sync_applies_total[5m])
```

**State pushes by event type**:

```promql
rate(lazylemon_state_sync_pushes_total[5m])
```

A healthy active-active cluster shows both `acquired` and `skipped` results
for `emit_claims`. If only one instance ever acquires claims, the other may
have lost its Redis connection or have a misconfigured `channel_prefix`.

## Trade-offs and Limitations

| Property                    | Behavior                                                                                                                                                                                                        |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Consistency model**       | Eventually consistent. Peers converge after each pub/sub notification. Under normal conditions this is sub-second.                                                                                              |
| **Conflict resolution**     | Last-write-wins on `Job.last_modified`. Simultaneous writes from multiple instances are resolved deterministically with no manual intervention.                                                                 |
| **Redis dependency**        | Redis becomes a required component when state sync is enabled. Redis downtime after startup causes degraded sync (warning logs) but does not stop processing — instances continue working on their local state. |
| **Duplicate dispatch risk** | On Redis failure during `try_claim_emit`, the instance proceeds (fail-open). A downstream idempotency guard is recommended for critical workflows.                                                              |
| **Crash recovery**          | Restarting instances reload state from Redis and resume without losing accumulated file groupings.                                                                                                              |
| **Stale claim keys**        | Claim keys expire via TTL. If the dispatching instance crashes before the TTL expires, the job will not be re-dispatched until the TTL elapses. Size the TTL to match your job processing time.                 |
| **Single Redis**            | There is no built-in Redis Sentinel or Cluster support. Point `host` at a Redis Sentinel-aware proxy or a load-balanced endpoint for Redis HA.                                                                  |
