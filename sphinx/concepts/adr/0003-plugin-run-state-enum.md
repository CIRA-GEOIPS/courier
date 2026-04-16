# ADR-0003: PluginRunState Enum for Plugin Lifecycle

## Status

Accepted

## Context

Plugin lifecycle originally used a `self._running: bool` flag (True = running,
False = stopped/failed). This conflates three distinct states — stopped, failed, and
restarting — into a single bit, making state transitions invisible in logs and
preventing the plugin manager from distinguishing a clean stop from a crash.

## Decision

Replace the boolean flag with a `PluginRunState` enum
(`src/courier/constants.py`) with values:
`STOPPED`, `STARTING`, `RUNNING`, `STOPPING`, `FAILED`, `RESTARTING`.

The `PluginManager` stores a `PluginRunState` per plugin in `PluginStateInfo.state`
and exports it as a Prometheus gauge. Base plugin interfaces (`DataMonitorBasePlugin`,
`JobBuilder`, `Dispatcher`) and `PluginManager` itself use `self._state: PluginRunState`
rather than `self._running: bool`.

## Alternatives Considered

- **Keep boolean**: Simpler, but loses diagnostic granularity and forces callers to
  infer restart state from thread liveness rather than an explicit enum value.
- **State machine class**: A `StateMachine` with transition guards (as described in
  `plugin-design.md`). More correct but significantly more code. Deferred until
  `InvalidTransitionError` is needed in practice.

## Trade-offs Accepted

- The `STARTING` and `STOPPING` states are defined but not yet set by the base plugin
  classes; they are reserved for a future `StateMachine` implementation.
- The enum is serialized as an integer in the Prometheus metric, not by name.
