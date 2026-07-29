# Design Concepts

This section explains Courier's architecture — how plugins fit together,
how messages flow through the pipeline, and why key design choices were
made the way they were.

## Architecture Decision Records (ADRs)

Courier's architecture is shaped by Architecture Decision Records (ADRs) — documents that capture non-obvious design choices, alternatives considered, and trade-offs accepted. Each ADR below covers a specific architectural decision.

```{toctree}
:maxdepth: 1

adr/0001-kombu-message-broker-abstraction
adr/0002-redis-ha-state-sync
adr/0003-plugin-run-state-enum
adr/0004-frozen-file-mutable-file-split
adr/0005-segregated-plugin-protocols
adr/0006-dispatcher-routing
adr/0007-behavioural-test-strategy
adr/0008-entry-point-plugin-discovery
```
