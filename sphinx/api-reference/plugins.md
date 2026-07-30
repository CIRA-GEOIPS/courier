# Plugins API Reference

Courier plugins implement one of three interfaces:
:class:`~courier.interfaces.data_monitors.DataMonitorBasePlugin`,
:class:`~courier.interfaces.job_builders.JobBuilder`, or
:class:`~courier.interfaces.dispatchers.Dispatcher`.

## Standard Data Monitors

### file_system_poller_watchdog

:class:`~courier.plugins.classes.data_monitors.file_system_poller_watchdog.FileSystemPoller`

Watches a directory for new files and emits them to the pipeline.

## Standard Dispatchers

### serial_bash

:class:`~courier.plugins.classes.dispatchers.serial_bash.SerialBashDispatcher`

Executes a single Jinja2-templated bash script for an entire job. All
files are available in the template, producing one
:class:`~courier.types.execution_log.ExecutionLog`.

See {doc}`plugins/serial_bash` for the full serial bash dispatcher
documentation, including configuration, template context, error handling,
and output file scanning.

### parallel_bash

:class:`~courier.plugins.classes.dispatchers.parallel_bash.ParallelBashDispatcher`

Executes a Jinja2-templated bash script independently for each file in
the job. Up to ``max_workers`` scripts run concurrently.

See {doc}`plugins/parallel_bash` for the full parallel bash dispatcher
documentation, including configuration, template context, error handling,
and output file scanning.

### slurm_dispatcher

:class:`~courier.plugins.classes.dispatchers.slurm_dispatcher.SlurmDispatcher`

Submits jobs to a SLURM cluster via ``sbatch`` using Jinja2-templated
job scripts.

.. literalinclude:: ../../../src/courier/plugins/classes/dispatchers/slurm_dispatcher.py
   :language: python
   :start-after: class SlurmDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:

(pipeline-feedback-example)=

### Pipeline Feedback with ``emit_file``

.. py:method:: Dispatcher.emit_file(file: File) -> None

All dispatchers inherit :meth:`emit_file` from the :class:`~courier.interfaces.dispatchers.Dispatcher`
base class. This method publishes an output :class:`~courier.types.file.File` to
:data:`~courier.constants.FILE_FOUND_EXCHANGE` — the same fanout exchange that
data monitors use — so downstream job builders can pick it up and create new
jobs.

This enables **chained pipeline workflows**: one dispatcher processes a job,
writes output files, then feeds those files back into the pipeline for a
second processing stage without any external intervention.

When to use
   Use ``emit_file()`` when your dispatcher produces output files that need
   further processing. Common patterns include:

   - **Multi-stage processing** — run a calibration stage, then feed
     calibrated files into a product-generation stage.
   - **Fan-out reprocessing** — emit derived products as new files so
     multiple downstream builders can route them to different dispatchers.
   - **Chained quality control** — a QC dispatcher inspects output and emits
     files that pass validation to the next stage while logging failures.

   The mechanism is identical to data monitor file emission: the file
   travels through ``FILE_FOUND_EXCHANGE``, job builders consume it, and
   routing works exactly as described in
   :doc:`../concepts/adr/0006-dispatcher-routing`.

How to use
   Override :meth:`~Dispatcher.get_execution_log` in your dispatcher
   subclass and call ``self.emit_file()`` for each output file you want to
   feed back into the pipeline:

   .. code-block:: python

      from courier.types.file import File

      class MyPipelineDispatcher(Dispatcher):
          def get_execution_log(self, job: Job) -> list[ExecutionLog]:
              # ... process the job, write output files ...
              output_path = Path("/data/output/processed.nc")
              self.emit_file(File(
                  file=output_path,
                  hostname=self.parent_service.hostname,
                  source=job.files[0].source,
                  instrument=job.files[0].instrument,
                  processing_stage="l2",
              ))
              return [ExecutionLog(return_code=0)]

Example: Multi-stage YAML configuration
   The example below shows a two-stage pipeline where ``serial_bash``
   produces calibrated files that a second job builder picks up and routes
   to ``parallel_bash`` for product generation:

   .. code-block:: yaml

      spec:
        run:
          # Stage 1: Calibrate raw files
          - calibrate:
              kind: dispatcher
              name: serial_bash
              config:
                bash_script: |
                  #!/bin/bash
                  # ... calibration logic ...
                  echo "calibrated.nc"  # signal output file path

          # Stage 2 builder: Only picks up calibrated files
          - build-products:
              kind: job_builder
              name: filter_and_group
              config:
                filters:
                  processing_stage: l2
                targets:
                  - generate-products

          # Stage 2: Generate products from calibrated files
          - generate-products:
              kind: dispatcher
              name: parallel_bash
              config:
                bash_script: |
                  #!/bin/bash
                  # ... product generation logic ...

   The key points:

   - Stage 1's dispatcher must call ``self.emit_file()`` with the output
     file path, setting ``processing_stage`` (or another metadata field)
     to a distinct value.
   - Stage 2's job builder uses a ``filters`` block to select only files
     from the calibration stage, preventing infinite loops.
   - The pipeline continues naturally — no custom wiring or external
     scripts needed.

.. note::

   **Avoid infinite loops.** Always set distinguishing metadata (e.g.,
   ``processing_stage``) on emitted files and use ``filters`` in downstream
   builders to prevent a file from being picked up by the same stage that
   produced it.

## Standard Job Builders

### DummyJobBuilder

:class:`~courier.plugins.classes.job_builders.dummy_job_builder.DummyJobBuilder`

Creates a minimal job for each file. Suitable for development and testing;
for production, use `filter_and_group` or a custom builder.

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

```yaml
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

```{include} ../includes/breaking-changes.md
```

See {doc}`types` for the `File`/`FrozenFile` attribute reference.

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

### MetadataRouterBuilder

:class:`~courier.plugins.classes.job_builders.metadata_router.MetadataRouterBuilder`

Routes files to different dispatchers based on file metadata (source, instrument, etc.).

```{toctree}
:maxdepth: 1
:hidden:

plugins/serial_bash
plugins/parallel_bash
```
