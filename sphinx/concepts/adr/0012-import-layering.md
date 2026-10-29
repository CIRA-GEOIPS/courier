# ADR-0012: Import Layering Enforced as a DAG

## Status

Proposed

## Context

Courier's packages are already close to layered: almost every import flows
from the applications down through the runtime to a small foundation, and no
core package imports `courier.plugins`. But the layering is a habit, not a
rule. Nothing checks it, and the places where it bends have accumulated:

- `courier/__init__.py` imports `courier.interfaces`, which re-exports
  `Service`. Importing any interface class therefore imports the broker, the
  managers and the tracing stack. Most of the local, function-level imports in
  the codebase exist to dodge the cycles this creates: `service.py` imports the
  interface ABCs at call time, and `interfaces.data_monitors` imports its own
  package from inside `__init__`.
- The plugin kind vocabulary (`normalize_kind`, `KIND_INFO`) lives in
  `courier.cli.plugins`, so a plugin interface and the dashboard generator both
  import the command-line layer to understand a config file.
- `courier.utils.metadata` imports `courier.types` and `courier.schema`, so
  `courier.types` cannot import `courier.utils` at module level and reaches for
  its datetime helper inside three functions instead.
- `courier.schema.v1alpha1.service_config` embeds the runtime
  `ServiceConfig`, so the YAML model depends on the settings object rather than
  the other way round.
- `courier.broker.kombu` subclasses `courier.managers.base.ServiceManager`
  while `courier.managers.plugin_manager` is the thing that would naturally sit
  above the broker, which leaves the two packages pointing at each other.
- The local and Slurm dispatchers import the three shipped payload classes by
  name. A third-party payload can never be executed by a shipped dispatcher.
- The RabbitMQ watcher imports `broker_error_triage` from the broker package,
  so one plugin depends on runtime infrastructure the others do not.
- The top-level `courier.dispatchers` package holds output-scanning helpers for
  the dispatcher *interface* and shares its name with the plugin family and the
  entry-point group.

None of these is a bug today. Together they mean the package graph cannot be
stated, so it cannot be defended in review, and the plugin boundary that
ADR-0008 built on entry points is held in place by nothing but convention.

This is alpha software. The question is what the graph should be, not how
little the current one has to move.

## Decision

The import graph is a directed acyclic graph with the edges below, enforced by
[import-linter](https://import-linter.readthedocs.io/) in pre-commit and CI.
Any edge not in the table is forbidden; the [contracts](#contracts) below say
which of those prohibitions the linter checks and which are left to review.

```{mermaid}
flowchart TB
    classDef app fill:#f3e8ff,stroke:#7e22ce,color:#000
    classDef plugin fill:#dcfce7,stroke:#15803d,color:#000
    classDef runtime fill:#dbeafe,stroke:#1d4ed8,color:#000
    classDef contract fill:#fef3c7,stroke:#b45309,color:#000
    classDef foundation fill:#f1f5f9,stroke:#475569,color:#000

    subgraph Apps
        cli
        dashboard
        viz
    end
    subgraph Plugins["Plugins (entry points only)"]
        plugins["plugins.*<br/>data_monitors | job_builders | dispatchers | payloads | data_monitor_configs"]
    end
    subgraph Runtime
        service
        managers
        broker
        routing
    end
    subgraph Contract
        interfaces
        sync
        lifecycle
    end
    subgraph Foundation
        tracing
        config
        schema
        types
        utils
        metrics
        constants
        errors
    end

    cli --> service & dashboard & viz & interfaces & routing & schema & config & utils
    dashboard --> schema
    viz --> constants
    plugins --> interfaces & schema & types & utils & tracing & metrics
    service --> managers & broker & routing & interfaces & tracing & config
    managers --> broker & interfaces & lifecycle & tracing & config
    broker --> lifecycle & config
    routing --> constants
    interfaces --> sync & schema & tracing & types & utils & metrics
    sync --> schema & types & utils & metrics
    lifecycle --> errors
    tracing --> utils & config
    config --> schema
    schema --> constants
    types --> utils
    utils --> constants
    metrics --> constants
    constants --> errors

    class cli,dashboard,viz app
    class plugins plugin
    class service,managers,broker,routing runtime
    class interfaces,sync,lifecycle contract
    class tracing,config,schema,types,utils,metrics,constants,errors foundation
```

The diagram omits edges to `errors`, `constants` and `metrics` where a node
already reaches them through a lower node; the table is authoritative.

### Allowed edges

| Node           | May import                                                                                                    | Reason                                                                                                                                  |
| -------------- | ------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `cli`          | `service`, `dashboard`, `viz`, `interfaces`, `routing`, `schema`, `config`, `utils`, `errors`, `constants`    | The composition root. The only node that may know every app and the runtime.                                                            |
| `dashboard`    | `schema`, `constants`, `errors`                                                                               | A pure generator from a config model to JSON. No CLI, no runtime.                                                                       |
| `viz`          | `constants`, `errors`                                                                                         | Reads Prometheus over HTTP; needs no courier runtime.                                                                                   |
| `plugins.*`    | `interfaces`, `schema`, `types`, `utils`, `tracing`, `metrics`, `errors`, `constants`                         | Plugins see the contract and the foundation, nothing else. Each family is independent of every other family, `payloads` included.       |
| `service`      | `managers`, `broker`, `routing`, `interfaces`, `tracing`, `config`, `constants`, `errors`, `metrics`, `utils` | The runtime assembles everything beneath it.                                                                                            |
| `managers`     | `broker`, `interfaces`, `lifecycle`, `tracing`, `config`, `utils`, `constants`, `errors`, `metrics`           | Managers own lifecycle; the broker manager is a manager, so `broker` sits below.                                                        |
| `broker`       | `lifecycle`, `config`, `utils`, `constants`, `errors`, `metrics`                                              | Transport only. Knows queues and connections, not plugins.                                                                              |
| `routing`      | `constants`, `errors`                                                                                         | Pure target resolution.                                                                                                                 |
| `interfaces`   | `sync`, `schema`, `tracing`, `types`, `utils`, `constants`, `errors`, `metrics`                               | The plugin contract: protocol, discovery, the four ABCs, and the output-scanning helpers. Never imports `service`.                      |
| `sync`         | `schema`, `types`, `utils`, `errors`, `metrics`                                                               | Shared-state plumbing for job builders.                                                                                                 |
| `lifecycle`    | `errors`                                                                                                      | New and tiny: the `ServiceManager` start/stop base, moved out of `managers.base` so `broker` and `managers` do not point at each other. |
| `tracing`      | `utils`, `config`                                                                                             | Observability wiring over logging.                                                                                                      |
| `schema`       | `constants`, `errors`                                                                                         | Pure Pydantic models of the YAML. Does not import `config`.                                                                             |
| `config`       | `schema`, `constants`, `errors`                                                                               | Runtime settings are derived from the schema, not embedded in it.                                                                       |
| `utils`        | `constants`, `errors`                                                                                         | Pure helpers. Never imports `types` or `config`.                                                                                        |
| `types`        | `utils`, `constants`, `errors`                                                                                | Data carriers that cross the broker. May use the datetime helpers.                                                                      |
| `metrics`      | `constants`                                                                                                   | Metric definitions.                                                                                                                     |
| `constants`    | `errors`                                                                                                      |                                                                                                                                         |
| `errors`       | nothing                                                                                                       |                                                                                                                                         |
| root `courier` | `_version`                                                                                                    | Exports `__version__` only. `Service` is imported from `courier.service`.                                                               |

Imports inside `if TYPE_CHECKING:` blocks are not edges. Plugins and
interfaces may annotate a `Service` parameter without depending on the
runtime.

### Contracts

Six contracts rather than one `layers` contract, because the graph is not a
line: `broker`, `managers` and `routing` are siblings, as are `sync` and
`lifecycle`. One layered list would forbid edges that are fine and permit edges
that are not. Each contract is its own entry so a failure names the rule it
broke.

1. **`core-does-not-import-plugins`** (`forbidden`). Every package except
   `courier.plugins` may not import `courier.plugins`. The only way core code
   reaches a plugin is through an entry point.
1. **`plugins-see-only-the-contract`** (`forbidden`). `courier.plugins` may
   not import `service`, `managers`, `broker`, `routing`, `sync`, `lifecycle`,
   `config`, `cli`, `dashboard` or `viz`.
1. **`plugin-families-are-independent`** (`independence`). The five plugin
   families do not import one another.
1. **`apps-are-leaves`** (`forbidden`). No node other than `cli` may import
   `cli`, `dashboard` or `viz`.
1. **`foundation`** (`layers`). `tracing` over `config` over
   `schema | types` over `utils | metrics` over `constants` over `errors`.
1. **`runtime`** (`layers`). `service` over `managers` over
   `broker | routing | interfaces` over `sync | lifecycle`.

A `layers` contract forbids upward imports and imports between siblings
separated by `|`. It does not forbid a higher layer from skipping down: it
permits `config -> types`, `tracing -> schema` and `managers -> routing`,
which the table leaves out. The contracts therefore guarantee the direction of
every edge, not the exact edge set. The table is the edge set reviewers hold a
change to; an omitted downward edge is caught in review, not by the linter,
until it is worth a `forbidden` contract of its own.

The `cli` module's lazy imports of `viz` and `dashboard`, which keep heavy
optional dependencies off the command-line start-up path, are declared under
`ignore_imports` rather than hoisted.

## Alternatives Considered

- **One `layers` contract over every package.** Rejected: it would either
  order `broker` against `managers` and `routing`, which have no reason to
  know each other, or leave real inversions unchecked.
- **`service` below `interfaces`.** The interfaces only name `Service` under
  `TYPE_CHECKING`, while `service` needs the ABCs at runtime for `isinstance`
  checks and registry lookups. Putting `service` underneath would mean moving
  those checks out of the service for no gain, and would make every interface
  import pull in the broker.
- **Allowing `plugins.dispatchers` to import `plugins.payloads`.** The shipped
  dispatchers do this today to recognise the shipped payloads. Keeping the edge
  would make it impossible for a third-party payload to be dispatched, which
  contradicts ADR-0008 and ADR-0011. The dispatcher's `representations` list
  is matched against a capability declared on the `Payload` ABC instead.
- **Allowing plugins to import `broker`.** One watcher wants
  `broker_error_triage`. Letting it in would let every plugin reach the
  transport; the helper moves below the broker instead.
- **Keeping `ServiceManager` in `managers.base`.** Then `broker` must import
  `managers` while `managers.plugin_manager` sits above the broker, which is a
  cycle at package level however it is drawn. A one-module `lifecycle`
  package is cheaper than the exception.
- **Ruff's `PLC0415` alone.** It flags function-level imports but cannot say
  which direction is wrong, and every one of the current sixty-odd sites
  carries a `noqa`. Import-linter checks the graph, not the syntax.

## Trade-offs Accepted

- **A new foundation package, `lifecycle`, with one class in it.** The cost
  is a package whose only job is to be beneath two others.
- **The `schema` and `config` inversion is the largest change.** The YAML model
  and the settings object are split, and every site that reads
  `ServiceConfigModel.spec.config` as a `ServiceConfig` is touched. It goes
  last so the other contracts protect the work while it happens.
- **`utils.metadata` moves up to `interfaces`** and becomes
  `interfaces.metadata`. It was never a utility; it applies data-monitor
  configs to a `Datum`.
- **`courier.dispatchers` is folded into `interfaces`** rather than renamed.
  Two importers, both already in `interfaces`.
- **`from courier.interfaces import Service` stops working.** Tests, docs and
  examples that do so are updated to `from courier.service import Service`.
- **Six contracts are more to read than one.** Each is short and named for
  the rule it enforces.

## Consequences

- Import-linter runs in pre-commit and as a CI job alongside ruff, mypy and
  vulture, against `src/`.
- Contracts 1, 2, 3 and 4 land first, as far as they pass against the current
  tree, so the plugin boundary is guarded before anything moves.
- The work then lands in this order, each step small enough to review alone:
  1. Kind vocabulary (`normalize_kind`, `KIND_INFO`) moves to `schema`.
  1. `_payload_blocks` moves out of `cli.run` so `cli.plugins` and `cli.run`
     stop importing each other.
  1. `lifecycle` is created and `ServiceManager` moves into it.
  1. `tracing` hoists its logging import; `utils.metadata` becomes
     `interfaces.metadata`; `types` imports `utils` at module level.
     Contract 5 lands.
  1. `courier.dispatchers` is folded into `interfaces`.
  1. `interfaces/__init__.py` stops re-exporting `Service`; the root package
     exports `__version__` only; `service.py` hoists its four runtime
     interface imports. Contract 6 lands.
  1. `broker_error_triage` moves below `broker`; the dispatcher-to-payload
     imports are replaced by a `Payload` capability. Contracts 2 and 3 are
     tightened to their final form.
  1. `config` is rebuilt on top of `schema`.
- A plugin that is installed from a third-party distribution has exactly the
  same import surface as a shipped one. Nothing in `courier.plugins` is
  special.
- The remaining function-level imports are `TYPE_CHECKING` guards and the two
  declared lazy imports in `cli`. New `noqa: PLC0415` sites need a reason that
  survives review.
