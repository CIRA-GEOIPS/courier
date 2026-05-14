# Plugins API Reference

Lazy Lemon plugins implement one of three interfaces:
:class:`~courier.interfaces.module_based.data_monitors.DataMonitor`,
:class:`~courier.interfaces.module_based.job_builders.JobBuilder`, or
:class:`~courier.interfaces.module_based.dispatchers.Dispatcher`.

## Standard Dispatchers

### serial_bash

:class:`~courier.plugins.classes.dispatchers.serial_bash.SerialBashDispatcher`

Executes a single Jinja2-templated bash script for an entire job. All
files are available in the template, producing one
:class:`~courier.types.execution_log.ExecutionLog`.

.. literalinclude:: ../../../src/courier/plugins/classes/dispatchers/serial_bash.py
   :language: python
   :start-after: class SerialBashDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:

### parallel_bash

:class:`~courier.plugins.classes.dispatchers.parallel_bash.ParallelBashDispatcher`

Executes a Jinja2-templated bash script independently for each file in
the job. Up to ``max_workers`` scripts run concurrently.

.. literalinclude:: ../../../src/courier/plugins/classes/dispatchers/parallel_bash.py
   :language: python
   :start-after: class ParallelBashDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:

### slurm_dispatcher

:class:`~courier.plugins.classes.dispatchers.slurm_dispatcher.SlurmDispatcher`

Submits jobs to a SLURM cluster via ``sbatch`` using Jinja2-templated
job scripts.

.. literalinclude:: ../../../src/courier/plugins/classes/dispatchers/slurm_dispatcher.py
   :language: python
   :start-after: class SlurmDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:

## Standard Job Builders

### DummyJobBuilder

:class:`~courier.plugins.classes.job_builders.dummy_job_builder.DummyJobBuilder`

Creates an empty job for each file — useful as a pass-through builder.

### filter_and_group

:class:`~courier.plugins.classes.job_builders.filter_and_group.FilterAndGroupJobBuilder`

Groups files into jobs by metadata filters and optional time windows.
Jobs are emitted when the file count reaches ``files_per_job``, or when a
``window_timeout_seconds`` has elapsed and at least ``min_files`` have
accumulated (dropout path).

.. literalinclude:: ../../../src/courier/plugins/classes/job_builders/filter_and_group.py
   :language: python
   :start-after: class FilterAndGroupJobBuilder(JobBuilder):
   :end-before:     interface: ClassVar[str] = "job_builders"
   :linenos:

#### Config Fields

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Default
     - Description
   * - ``files_per_job``
     - ``int``
     - ``5``
     - Number of files that triggers job emission (fast path). Minimum ``1``.
   * - ``min_files``
     - ``int``
     - ``1``
     - Minimum files required before the dropout path fires. Must be ``<= files_per_job``.
   * - ``window_timeout_seconds``
     - ``float`` | ``None``
     - ``None``
     - Seconds since the first file after which a partial job may be emitted. When ``None``, the dropout path is disabled entirely.
   * - ``filters``
     - ``dict[str, str]``
     - ``{}``
     - Key-value pairs that each file must satisfy (see :ref:`filter-syntax`).
   * - ``time_grouping``
     - ``dict[str, Any]`` | ``None``
     - ``None``
     - Optional time-bucketing configuration. Supports keys ``weeks``, ``hours``, ``minutes``, ``seconds`` (``float``) and ``start`` (ISO-8601 string or ``datetime``). Files are assigned to a bucket ID based on their ``timestamp``.
   * - ``targets``
     - ``list[str]`` | ``None``
     - ``None``
     - Dispatcher identifiers this builder's jobs are published to. ``None`` is resolved at preflight via the service's ``allow_implicit_target`` policy.

#### Filter Syntax

.. _filter-syntax:

Each key-value pair in the ``filters`` dict is checked against each file
with a **two-layer lookup**:

1. **Metadata layer** — ``file.metadata.get(key)``. Keys stored in the
   metadata dict (populated from ``field_map`` entries that do not map to a
   named ``File`` attribute) are checked first.

2. **Attribute layer** — ``getattr(file, key, None)``. If the key is not
   found in metadata, the ``File`` dataclass attributes (``source``,
   ``instrument``, ``processing_stage``, ``domain``, ``hostname``,
   ``num_expected``, ``timestamp``) are checked.

If the key is found in **neither** layer, a ``WARNING`` is logged and the
file is rejected (the filter returns ``False``).

```python
# Example: match GOES-16 ABI L1b full-disk files
filters:
  source: goes16
  instrument: abi
  processing_stage: l1b
  domain: full-disk
```

#### Breaking Change: Filter Key Names

Filter configurations **must use ``File`` attribute names**, not legacy
field_map names. The following legacy keys are no longer recognized:

.. list-table::
   :header-rows: 1

   * - Legacy Key
     - Replace With
   * - ``platform``
     - ``source``
   * - ``sensor``
     - ``instrument``
   * - ``level``
     - ``processing_stage``
   * - ``sector``
     - ``domain``

**Migration example:**

```yaml
# Before
filters:
  platform: goes16
  sensor: abi
  level: l1b

# After
filters:
  source: goes16
  instrument: abi
  processing_stage: l1b
```

## Standard Data Monitors

### file_system_poller_watchdog

:class:`~courier.plugins.monitors.file_system_poller_watchdog.FileSystemPollerWatchdog`

Watches a directory for new files and emits them to the pipeline.

### MetadataRouterBuilder

:class:`~courier.plugins.classes.job_builders.metadata_router.MetadataRouterBuilder`

Routes files to different dispatchers based on file metadata (source, instrument, etc.).
