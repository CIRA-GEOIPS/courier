# ADR-0008: Entry-Point Plugin Discovery

## Status

Accepted. Supersedes the plugin-discovery half of {doc}`0005-segregated-plugin-protocols`.

## Context

Courier discovered plugins through `pluginify`, a proprietary library that
scanned `<package>/plugins/**` for `.py` and `.yaml` files, wrote a
`registered_plugins.json` cache into a platformdirs cache directory, and needed
a `pluginify create` shell-out to build it. That shell-out ran in the Typer
callback on **every** CLI invocation, and was duplicated in CI and the
devcontainer.

Four problems, in ascending order of severity.

**It coupled us to a cache for information the interpreter already has.**
Discovery required three environment variables (`PLUGINIFY_NAMESPACE`,
`PLUGINIFY_REBUILD_REGISTRIES`, `PLUGINIFY_REGISTRY_DIRECTORY`), a config file
at `~/.config/pluginify/config.yaml`, and a writable cache directory. A
third-party plugin package had to adopt courier's namespace convention *and*
trigger a registry rebuild.

**Registries returned instances where every caller wanted a class.**
`create_service_with_plugins` is typed
`Sequence[tuple[type[ServicePlugin], dict, str | None]]`, so `cli/run.py` did
`type(registry.get_plugin(name))` to undo the construction. Supporting that
construction meant every plugin `__init__` had to accept a module and bail out:

```python
def __init__(self, service: Service | types.ModuleType | None = None, ...):
    # pluginify registration path: instantiated with only a module
    if service is None or isinstance(service, types.ModuleType):
        return
```

That guard appeared in 17 files, alongside a `call()` stub in each of the three
bases that existed only for pluginify's signature validator and was never
invoked.

**It re-executed plugin files instead of importing them.** pluginify loaded each
plugin with `spec_from_file_location` + `exec_module`, twice. The class it handed
back was therefore a *different object* from the one an ordinary import produced:

```
>>> data_monitors.get_plugin("cron_glob").__class__ is cron_glob.CronGlob
False
```

Both report `__module__ == "courier.plugins.data_monitors.cron_glob"`, so
nothing about the duplication is visible until something does an identity or
`isinstance` check against the imported class. `find_config_model` survived only
because it re-imports by module name and compares strings. This was a latent
defect, not a stylistic complaint.

**The YAML plugin format carried an envelope nobody read.** The seven satellite
metadata configs each declared `apiVersion`, `interface`, `family`, `kind`,
`description`, and `docstring`. Six of the eight fields on `DataMonitorConfig`
existed to satisfy discovery; the only consumer,
`apply_metadata_from_configs`, reads `spec` and nothing else. Two of the model's
validators were YAML-lint rules — one required `description` to start with a
capital and end with a period.

## Decision

Discover every plugin through `importlib.metadata.entry_points`, with one group
per interface, and delete the YAML plugin format entirely.

### Groups

`courier.data_monitors`, `courier.job_builders`, `courier.dispatchers` resolve to
plugin **classes**. `courier.data_monitor_configs` resolves to validated
`DataMonitorConfig` **instances** — a config is data, not behaviour, and building
it at import means a malformed one fails discovery rather than silently matching
no files.

The entry-point name is the name operators write in a service config. It is
declared twice — once as the key, once as the class's `name` — and a test pins
their agreement.

### Registries return classes

`courier.interfaces.discovery` provides two small frozen dataclasses over one
cached entry-point reader. This deletes the `type(...)` round-trip at both call
sites, all 17 `types.ModuleType` guards, the three `call()` stubs, and the
`PLUGIN_CLASS` sentinel in all 14 plugin modules. It also collapses the
duplicate-class defect: entry points go through the normal import system, so
`get_plugin("cron_glob") is cron_glob.CronGlob`.

### Listing does not import

`names()` reads metadata only. `courier plugins list` therefore imports no plugin
module, which matters because plugins depend on optional extras (`boto3`,
`kafka-python`, `paramiko`) and one broken or uninstallable plugin would
otherwise take the whole listing down.

### Metadata configs are Python

`DataMonitorConfig` keeps `name` and `spec`; the six envelope fields and the two
YAML-lint validators are gone, and `extra="allow"` becomes `extra="forbid"`.
Description lives in the declaring module's docstring.

### `data_monitor_configs` is not a runnable kind

It was accepted in `spec.run` and then specially skipped in two places — a config
posing as a pipeline step. No config anywhere used it. In its place, an
unrecognised `kind` now **raises**; previously it was skipped silently, producing
a service that started, reported healthy, and processed nothing.

## Alternatives Considered

- **Keep the YAML format, discovered via a resource-package entry point.**
  Rejected. It preserved drop-in-a-file authoring, but at the cost of two plugin
  formats, two loaders, and a schema whose envelope existed only to serve the
  format. One mechanism is worth the migration.

- **Declare in `[tool.poetry.plugins]` only.** Rejected, but the reasoning is
  worth recording because the failure mode is silent. The two tables are
  *alternatives, not additive*: poetry-core 1.x ignores `[project]` and reads the
  poetry table; 2.x reads `[project.entry-points]` and ignores the poetry table
  entirely. Adding the new groups to the poetry table alone silently dropped
  `runcourier.dev.plugin_packages` from the built metadata and broke discovery on
  the spot. Both tables now carry every group, and `test_pyproject_tables_agree`
  is what keeps them honest.

- **Entry points plus a filesystem scan for first-party plugins.** Rejected.
  It removes the reinstall step during development, at the price of two discovery
  paths that can disagree — the class of bug this ADR exists to remove.

- **A `COURIER_PLUGIN_PATH` escape hatch.** Rejected for now as unearned
  surface; reconsider if the reinstall step proves genuinely painful.

## Trade-offs Accepted

- **Adding a plugin needs a reinstall.** Entry points live in installed
  distribution metadata. A new plugin file is importable and unit-testable while
  remaining invisible to `courier run`. This is the real cost, and it is paid
  during development rather than in production. Mitigated by
  `test_plugin_classes_on_disk_are_declared` and
  `test_declared_plugins_are_installed`, whose failure message is `Re-run: pip
  install -e .`.

- **Two toml tables to keep in sync**, for the backend reason above.

- **Metadata configs are no longer editable by non-programmers.** They were YAML;
  they are now Python. The content is regexes either way, so the audience was
  already narrow, but this is a real reduction in accessibility.

- **Dropping the config envelope is a breaking schema change.** An out-of-tree
  `DataMonitorConfig` built from a dict containing `apiVersion` / `interface` /
  `family` now fails against `extra="forbid"`.

## Consequences

- `pluginify` is uninstalled and the full suite passes without it. Warnings
  emitted during a test run fell from 364 to 1.
- No cache directory, no `PLUGINIFY_*` variables, no `pluginify create` in CI or
  the devcontainer, and no registry bootstrap on the CLI's hot path.
- A third-party plugin package is one entry point and a `pip install`. See
  {doc}`../../contribute/writing-a-plugin`.
- Nine drift guards cover the declarations, each verified to fail on the mistake
  it describes rather than assumed to work.
- The satellite metadata conversion was done by script, not by hand, and gated on
  a temporary test comparing validated `Spec` objects against the YAML for all
  seven configs. Those files are where `M6C[01][1-6]` silently dropped ABI
  channels 7-10, fixed in `goes18` only; re-typing regexes is exactly how that
  happens.
