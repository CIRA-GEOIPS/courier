# ADR-0005: Segregated Plugin Protocols

## Status

Proposed

## Context

The monolithic `ServicePlugin` protocol (`interfaces/plugin_protocol.py`) requires
every plugin to implement `start()`, `stop()`, `is_healthy()`, and `get_metrics()`.
This violates Interface Segregation: a plugin that does not support health checks is
forced to implement `is_healthy()` — typically as `return True`, which is misleading.

The `plugin-design.md` design rule specifies three segregated protocols:
`Startable`, `HealthCheckable`, `MetricsProvider`.

## Decision

Keep the monolithic `ServicePlugin` protocol as-is for now. The `PluginManager`
currently assumes all four methods are present; refactoring to protocol-checking
(`isinstance(plugin, HealthCheckable)`) requires concurrent changes to
`PluginManager` and all existing plugins.

The `HealthCheckable` sub-protocol will be introduced when the first plugin needs
to opt out of health checking.

## Alternatives Considered

- **Immediate full segregation**: Correct but requires touching every plugin and the
  plugin manager simultaneously; high risk of introducing regressions.
- **Default no-op implementations in base classes**: `is_healthy()` returns `True`
  by default in `DataMonitorBasePlugin`. Pragmatic but misleads operators.

## Trade-offs Accepted

- `is_healthy()` in the three base plugins (`DataMonitorBasePlugin`, `JobBuilder`,
  `Dispatcher`) checks `self._state == PluginRunState.RUNNING`, which is meaningful
  but not a deep health check. Subclasses should override for real health validation.
- This ADR will be superseded when the `PluginManager` is updated to use
  `isinstance` protocol checks.
