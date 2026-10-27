# ADR-0013: A Plugin Blocked in start() Ends the Service

## Status

Accepted.

## Context

Each plugin is started on its own thread by the plugin manager. The base
`start()` of every family only spawns a worker thread and returns, but a job
builder with state sync connects to Redis, subscribes, and publishes any jobs
found already complete, and a Slurm dispatcher probes for tools on `PATH`.
Each of those is a call into something outside the process: a Redis that
accepts a connection and never answers, a broker confirm that never comes, a
network filesystem that hangs. A Python thread blocked in such a call cannot be
interrupted from outside.

Before this decision the manager had no opinion about a `start()` that never
returned. The plugin stayed `STARTING`, which health treated as "in transit",
so the service reported healthy forever with a plugin that would never do any
work. Nothing in the logs said why.

## Decision

A `start()` that has not returned within `plugin_start_timeout` (default 60
seconds, env `PLUGIN_START_TIMEOUT`) is given up on:

- The plugin is marked `FAILED` with its restart attempts used up, so the
  service's heartbeat raises `PluginFailedError` and the process exits
  non-zero. Whatever runs the process decides what happens next.
- The stuck thread's stack is written to the error log, so the operator
  learns what it was waiting on without attaching a debugger.
- The plugin's own `stop()` is called on a separate thread that the monitor
  does not wait for, in case the blocked call ever returns and the plugin
  should then wind down rather than carry on. If `start()` does return late,
  the plugin stays `FAILED`.
- The stuck thread and the stop thread are recorded on the plugin's
  `abandoned_threads`, and the status report counts those still running.
  They are not forgotten, because they cannot be freed.
- A start past its timeout counts as unhealthy, so the service's own
  post-startup health check also catches it.

Only the `start()` call is timed. Once it returns, the start gate's wait for
the plugin to report healthy has its own deadline,
`plugin_health_check_interval`, and does not count against
`plugin_start_timeout`.

## Alternatives Considered

**Restart the plugin.** Retrying a start that hung only stacks a second
blocked thread on the same dependency, and the first is still there. The
restart budget would be spent with nothing gained.

**Consume one restart attempt per timeout** rather than all of them, so a
transiently slow dependency gets another chance. Rejected for the same reason:
the first thread stays blocked either way. If the dependency recovers, a fresh
process starts cleanly; this one cannot.

**Run `start()` with a hard timeout** by giving each plugin a subprocess or a
cancellable task. Out of proportion for the problem, and it would change the
plugin contract for every family.

## Trade-offs Accepted

A dependency that is merely slow, past the timeout, now ends the service where
before it would have eventually come up. The default of 60 seconds is long for
that reason, and the timeout is configurable.

The exit happens on the next heartbeat after the monitor notices, so up to
`heartbeat_interval` plus `plugin_health_check_interval` after the timeout.

## Consequences

Operators get a log line naming the blocked function and a non-zero exit,
instead of a healthy-looking service that does nothing. The orchestrator's
restart policy applies, as it does for any other crash.
