# Writing a Plugin

Courier discovers plugins through Python [entry points]. A plugin is an ordinary
class in an ordinary module, declared in `pyproject.toml`. There is no registry
cache to rebuild and no naming convention to obey beyond the entry-point group.

## The five groups

| Group                          | What it holds                 | Base class                          |
| ------------------------------ | ----------------------------- | ----------------------------------- |
| `courier.data_monitors`        | Watch for data and emit files | `DataMonitorBasePlugin`             |
| `courier.job_builders`         | Group files into jobs         | `JobBuilder`                        |
| `courier.dispatchers`          | Execute jobs                  | `Dispatcher`                        |
| `courier.payloads`             | What a job executes, and how  | `Payload`                           |
| `courier.data_monitor_configs` | Filename-to-metadata rules    | `DataMonitorConfig` (an *instance*) |

The first four resolve to a **class**, which courier instantiates with the
service, the step's config, and its identifier. The fifth resolves to an
already-validated **instance**, because a metadata config is data.

Payloads are sub-plugins: they are never steps of their own in `spec.run`.
Every job builder nests exactly one, under `config.payload`, and its jobs carry
the rendered payload to whichever dispatcher runs them. See
{doc}`../api-reference/payloads` for the configuration a payload takes and
{doc}`../concepts/adr/0011-dispatchers-execute-jobs` for the design.

## A payload, end to end

A payload plugin decides how a job's script is launched. The shipped ones
(`shell_payload`, `bash_payload`, `python_payload`) form a hierarchy, and a new
one usually subclasses the closest of them. This one runs its script with
`zsh`:

```python
# my_package/zsh.py
"""Payload that runs its script with zsh."""

from typing import ClassVar

from courier.plugins.payloads.shell_payload import ShellPayload


class ZshPayload(ShellPayload):
    """Run the rendered script with ``zsh``."""

    interface: ClassVar[str] = "payloads"
    name: ClassVar[str] = "zsh_payload"
    version: ClassVar[str] = "1.0.0"
    default_binary: ClassVar[str] = "zsh"
    file_suffix: ClassVar[str] = ".zsh"
```

Everything else is inherited: `zsh [prefix_args...] <script> [suffix_args...]`
as the command, `binary` mode, and a `toolchain` check with
`zsh -c 'command -v <tool>'`.

Two rules apply to every payload class:

- **Put setup in `_configure_from_config`, not `__init__`.** The builder
  constructs the payload normally, but a dispatcher rebuilds it from the job
  with `Payload.from_job_spec`, which does not call `__init__`. Both paths run
  `_configure_from_config`.
- **Extra config fields go on a `PayloadConfig` subclass** named by the
  class's `config_class`. Both paths validate with it; unknown keys are
  rejected.

A dispatcher runs a payload as the most specific class in its hierarchy that
the dispatcher lists in `representations`. The shipped dispatchers list
`ShellPayload`, `BashPayload` and `PythonPayload`, so they would run a
`zsh_payload` as a `ShellPayload`, with `sh`. To run it as itself, a dispatcher
must list `ZshPayload`, like the one below.

## A dispatcher, end to end

A dispatcher decides where and how a payload runs. The base `Dispatcher` does
the work common to all of them: hydrating the payload a job carries, writing
its script, running the command, scanning output and publishing results. A
new dispatcher declares which payload classes it runs and changes only what
differs. This one runs every job at a lower CPU priority:

```python
# my_package/nice.py
"""Dispatcher that runs payloads on this host at a lower CPU priority."""

from typing import ClassVar

from pydantic import Field

from courier.interfaces.dispatchers import Dispatcher, ExecutionPayload
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.job import Job
from my_package.zsh import ZshPayload


class NiceDispatcherConfig(DispatcherGroupConfig):
    """Every dispatcher option, plus the priority to run payloads at."""

    niceness: int = Field(default=10, ge=0, le=19)


class NiceDispatcher(Dispatcher):
    """Run each job's payload under ``nice``."""

    interface: ClassVar[str] = "dispatchers"
    name: ClassVar[str] = "nice_dispatcher"
    version: ClassVar[str] = "1.0.0"

    # The payload classes this dispatcher runs.
    representations: ClassVar[list[type[Payload]]] = [
        ShellPayload,
        BashPayload,
        PythonPayload,
        ZshPayload,
    ]
    # The model its config block is validated with.
    config_class: ClassVar[type[DispatcherGroupConfig]] = NiceDispatcherConfig
    config: NiceDispatcherConfig

    def initialize_environment(self, job: Job, payload: Payload) -> ExecutionPayload:
        """Prepare the job as usual, then run its command under ``nice``."""
        env = super().initialize_environment(job, payload)
        env.command = ["nice", "-n", str(self.config.niceness), *env.command]
        return env
```

`representations` is required: a dispatcher that lists no payload classes can
run nothing, and preflight rejects every job builder that targets it.
`config_class` is needed only for options of your own; unknown keys are then
reported against your model.

The other hooks, from the outside in:

- `initialize_environment(job, payload)` returns the `ExecutionPayload`
  (command, script path, logging destinations) for a job, as above.
- `_execute_job(job, payload, env)` runs it. `local_dispatcher` extends it to
  read `COURIER_METRIC:` lines; `slurm_dispatcher` replaces it to submit to
  Slurm. Set `env.keep_file = True` if the script must outlive the call.
- `_collect_output_files(job, logs)` returns the files to feed back into the
  pipeline once the job has run; the base returns the `output_files`
  matches. Extend its list to emit files decided in code. The base class
  publishes them, then the execution logs, outside the job's error handling,
  so a broker fault while publishing is retried rather than counted as a
  failed job. Do not call `emit_file` from `_execute_job`: a fault there is
  contained as a failure of the job. See {ref}`pipeline-feedback-example`.
- `_dispatcher_context(script_path)` supplies the values of the reserved
  template names (`dispatcher`, `script_path`, `hostname`, and `output_dir`
  where it exists). Extend it to add fields under `dispatcher`, which a
  template reaches as `{{ dispatcher.<field> }}`; a template cannot use any
  other top-level name for a dispatcher value.

Overriding `get_execution_log` itself skips payload hydration, toolchain
checks and script handling, so prefer the hooks above.

Declare both plugins:

```toml
[project.entry-points."courier.payloads"]
zsh_payload = "my_package.zsh:ZshPayload"

[project.entry-points."courier.dispatchers"]
nice_dispatcher = "my_package.nice:NiceDispatcher"
```

Install, and courier can see them:

```bash
pip install -e .
courier plugins list
```

Then reference them from a service config by their entry-point names. The
payload is nested under the job builder; the dispatcher is a step:

```yaml
spec:
  run:
    - build:
        kind: job_builder
        name: DummyJobBuilder
        config:
          targets:
            - run-nicely
          payload:
            greet:
              kind: payload
              name: zsh_payload
              config:
                script: |
                  print -r -- "Processing {{ files[0].file }} with zsh $ZSH_VERSION"

    - run-nicely:
        kind: dispatcher
        name: nice_dispatcher
        config:
          niceness: 15
          log_to_logger: true
```

The payload plugin must be installed wherever a builder that nests it runs,
and wherever a dispatcher that receives its jobs runs.

## A job builder, end to end

A job builder decides which files go into a job. What its jobs execute is its
payload, so **every job builder requires a `payload` block**, and the
`JobBuilder` base class enforces it for yours as for the shipped ones. This
one emits a job for every NetCDF file:

```python
# my_package/netcdf.py
"""Job builder that makes one job per NetCDF file."""

from typing import ClassVar

from courier.interfaces.job_builders import PAYLOAD_KEY, JobBuilder
from courier.types.job import Job, JobGroup

#: The builder's own settings, kept out of the config every job carries: the
#: payload block (which travels as `job.payload`) and `state_sync` (Redis
#: connection settings, a password among them).
BUILDER_ONLY = frozenset({PAYLOAD_KEY, "state_sync"})


class OneFileJob(Job):
    """A job that is ready as soon as it holds one file."""

    def ready(self) -> bool:
        return len(self.files) == 1


class NetcdfGroup(JobGroup):
    """Accept ``.nc`` files, one job each."""

    def __init__(self, config: dict) -> None:
        super().__init__("netcdf", config)
        self.job = OneFileJob

    def file_is_relevant(self, file) -> bool:
        return str(file.file).endswith(".nc")


class NetcdfJobBuilder(JobBuilder):
    """Emit one job for every NetCDF file."""

    interface: ClassVar[str] = "job_builders"
    name: ClassVar[str] = "netcdf_builder"
    version: ClassVar[str] = "1.0.0"

    def __init__(self, service, config=None, identifier=None) -> None:
        # First: validates the `payload` block, or raises.
        super().__init__(service, config, identifier=identifier)
        # The group config travels in every job as `job.config`.
        group_config = {k: v for k, v in self.config.items() if k not in BUILDER_ONLY}
        self.job_groups = [NetcdfGroup(group_config)]
```

Four rules follow from the payload requirement:

- **Call `super().__init__` first.** `JobBuilder.__init__` validates the
  `payload` block before it sets up anything else. A block that is missing,
  is not a mapping, does not nest exactly one plugin, or nests one whose
  `kind` is not `payload` raises `InvalidPluginConfigError` (a
  `ConfigurationError`) naming the builder and showing a minimal block, so
  `courier run` stops at startup; see {doc}`../api-reference/plugins`. The
  validated block is kept as `self.payload_block`, its identifier as
  `self.payload_identifier`.
- **Let `payload` through your own config checks.** A builder that validates
  its config with a model that forbids unknown keys must allow `payload`.
- **Keep the block out of the jobs' config.** A job group's config travels in
  every job message as `job.config`, which is also the templates' `config`.
  The payload travels separately, rendered once, as `job.payload`. Drop
  `PAYLOAD_KEY` from the group config, as above, or build the group config
  from a model that keeps only its own fields. Drop `state_sync` too: it
  holds the Redis password.
- **Emit with `self.emit(job)`.** It renders the bound payload onto the job
  and publishes it; never set `job.payload` yourself.

The payload plugin itself is bound after construction: at startup, service
preflight assigns the plugin registered under `payload_identifier` to
`builder.payload`. Until one is bound, `start()` and `emit()` raise
`ConfigurationError` before consuming or publishing anything, and reading
`builder.payload` raises too; `builder.has_payload` asks without raising. The
setter accepts only a `Payload` whose identifier is `payload_identifier`. A
test that drives a builder without a service binds one itself:

```python
# tests/test_netcdf.py
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.errors import ConfigurationError, InvalidPluginConfigError
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import File
from courier.types.job import Job
from my_package.netcdf import NetcdfJobBuilder, OneFileJob

CONFIG = {
    "targets": ["process"],
    "payload": {
        "convert": {
            "kind": "payload",
            "name": "bash_payload",
            "config": {"script": 'convert.sh "{{ files[0].file }}"'},
        },
    },
}


def test_rejects_a_config_without_a_payload_block():
    with pytest.raises(InvalidPluginConfigError, match="needs a payload block"):
        NetcdfJobBuilder(MagicMock(config=None), {"targets": ["process"]})


def test_refuses_to_start_without_a_bound_payload():
    builder = NetcdfJobBuilder(MagicMock(config=None), CONFIG, identifier="build")
    with pytest.raises(ConfigurationError, match="no payload bound"):
        builder.start()


def test_every_job_carries_the_rendered_payload():
    service = MagicMock(config=None)
    builder = NetcdfJobBuilder(service, CONFIG, identifier="build")
    # What service preflight does: bind the payload the block names.
    builder.payload = BashPayload(
        service,
        builder.payload_block.spec.config,
        identifier=builder.payload_identifier,
    )
    group = builder.job_groups[0]
    job = OneFileJob(name=group.name, identifier="a", config=group.config)
    job.add_file(File(file=Path("/data/a.nc")))

    builder.emit(job)

    sent = Job.from_string(service.emit.call_args.kwargs["message"])
    assert sent.payload.script == 'convert.sh "/data/a.nc"'
    assert "payload" not in sent.config
```

Declare it in `courier.job_builders` and give every step that uses it a
payload block:

```toml
[project.entry-points."courier.job_builders"]
netcdf_builder = "my_package.netcdf:NetcdfJobBuilder"
```

```yaml
- build:
    kind: job_builder
    name: netcdf_builder
    config:
      targets:
        - process
      payload:
        convert:
          kind: payload
          name: bash_payload
          config:
            script: |
              convert.sh "{{ files[0].file }}"
```

## Three rules the tests enforce

**The entry-point key must equal the class's `name`.** They are two independent
declarations of the same string. If they disagree, discovery succeeds but the
plugin registers under the class's name, so queues and metrics label it
differently from the config that asked for it.

**`interface` must match the group.** A dispatcher in the `job_builders` group
would be constructed as the wrong kind of thing.

**You must reinstall after declaring a plugin.** Entry points live in installed
distribution metadata, not in your source tree. This is the one real cost of
entry points over a filesystem scan, and it bites during development: a new
plugin file is importable and unit-testable while still being invisible to
`courier run`.

For courier's own plugins, `tests/test_shipped_config_drift.py` catches all
three. `test_declared_plugins_are_installed` fails with the fix in the message:

```text
FAILED test_declared_plugins_are_installed[courier.dispatchers]
  courier.dispatchers: ['nice_dispatcher'] declared in pyproject.toml but missing
  from installed metadata.
  Re-run:  pip install -e .
```

```{note}
Courier declares its own plugins in **both** `[tool.poetry.plugins."courier.*"]`
and `[project.entry-points."courier.*"]`. The two tables are alternatives, not
additive: poetry-core 1.x reads the former and ignores `[project]`, while 2.x
reads the latter and ignores the poetry table entirely. Declaring both means a
wheel built by either backend ships every plugin. Your own package needs only
whichever table its build backend understands.
```

## A metadata config

Metadata configs are declared the same way, but the entry point names a
constructed object rather than a class:

```python
# my_package/configs/goes17_abi.py
"""Metadata for GOES-17 ABI L1B files."""

from courier.schema import DataMonitorConfig

CONFIG = DataMonitorConfig(
    name="goes17_abi",
    spec={
        "file_metadata": {
            "goes17_abi_l1b": {
                "source": "goes17",
                "instrument": "abi",
                "processing_stage": "L1B",
                "date": r".*s(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2}).*",
                "match": [r".*M6C(0[1-9]|1[0-6]).*"],
            },
        },
    },
)
```

```toml
[project.entry-points."courier.data_monitor_configs"]
goes17_abi = "my_package.configs.goes17_abi:CONFIG"
```

Building the model at import time is deliberate: a malformed config raises when
courier loads it, rather than quietly matching no files at run time. `spec`
forbids unknown keys for the same reason.

A data monitor uses it by name:

```yaml
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/incoming
          metadata-tools:
            - goes17_abi
```

## Listing is cheap; loading is not

`courier plugins list` reads entry-point metadata only and imports nothing. That
matters because plugins may depend on optional extras — `s3_poller` needs
`boto3`, `kafka_consumer` needs `kafka-python` — and an eager listing would
either import them all or fail on the first one missing.

Plugins are imported when a config actually names one. Keep expensive or
optional imports inside methods rather than at module scope, as the shipped
plugins do:

```python
def _client(self):
    import boto3  # noqa: PLC0415

    return boto3.client("s3")
```

## Next Steps

- {doc}`code-style` — the conventions courier's own code follows
- {doc}`../concepts/adr/0008-entry-point-plugin-discovery` — why discovery works
  this way
- {doc}`../concepts/adr/0007-behavioural-test-strategy` — what a good test for
  your plugin looks like

[entry points]: https://packaging.python.org/en/latest/specifications/entry-points/
