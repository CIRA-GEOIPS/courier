# ADR-0003: PluginRunState Enum for Plugin Lifecycle

## Status

Accepted

## Context

Plugin lifecycle originally used a `self._running: bool` flag (True = running,
False = stopped/failed). This conflates three distinct states — stopped, failed, and
restarting — into a single bit, making state transitions opaque to operators and
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

## Consequences

- **State visibility**: The `PluginManager` now stores and exports a typed
  `PluginRunState` per plugin via the `courier_plugin_state` Prometheus gauge, replacing
  the opaque boolean flag. Operators can distinguish `FAILED` from `STOPPED` at a
  glance.
- **Codebase impact**: All base plugin interfaces (`DataMonitorBasePlugin`,
  `JobBuilder`, `Dispatcher`) and the `PluginManager` itself use `self._state:
  PluginRunState` instead of `self._running: bool`. Plugin authors writing custom
  plugins should use the enum rather than a boolean.
- **Forward compatibility**: `STARTING` and `STOPPING` enum values are reserved for a
  future `StateMachine` implementation with transition guards.
- **Tracing visibility**: These states are surfaced as span events in distributed tracing. See {doc}`../../operations/tracing` for the span events reference.
