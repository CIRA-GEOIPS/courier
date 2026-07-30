# Writing a Plugin

Courier discovers plugins through Python [entry points]. A plugin is an ordinary
class in an ordinary module, declared in `pyproject.toml`. There is no registry
cache to rebuild and no naming convention to obey beyond the entry-point group.

[entry points]: https://packaging.python.org/en/latest/specifications/entry-points/

## The four groups

| Group | What it holds | Base class |
| --- | --- | --- |
| `courier.data_monitors` | Watch for data and emit files | `DataMonitorBasePlugin` |
| `courier.job_builders` | Group files into jobs | `JobBuilder` |
| `courier.dispatchers` | Execute jobs | `Dispatcher` |
| `courier.data_monitor_configs` | Filename-to-metadata rules | `DataMonitorConfig` (an *instance*) |

The first three resolve to a **class**, which courier instantiates with the
service, the step's config, and its identifier. The fourth resolves to an
already-validated **instance**, because a metadata config is data.

## A dispatcher, end to end

```python
# my_package/shout.py
"""Dispatcher that shouts each job to the log."""

from typing import ClassVar

from courier.interfaces.dispatchers import Dispatcher
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job


class ShoutDispatcher(Dispatcher):
    """Log every file in a job, loudly."""

    interface: ClassVar[str] = "dispatchers"
    name: ClassVar[str] = "shout"
    version: ClassVar[str] = "1.0.0"

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        for file in job.files:
            self._logger.info("SHOUTING ABOUT %s", file.file)
        return [ExecutionLog(return_code=0, stdout="", stderr="", hostname="local")]
```

Declare it:

```toml
[project.entry-points."courier.dispatchers"]
shout = "my_package.shout:ShoutDispatcher"
```

Install, and courier can see it:

```bash
pip install -e .
courier plugins list
```

Then reference it from a service config by the entry-point name:

```yaml
spec:
  run:
    - shout-about-it:
        kind: dispatcher
        name: shout
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
  courier.dispatchers: ['shout'] declared in pyproject.toml but missing
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
