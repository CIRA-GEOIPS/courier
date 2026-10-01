# Plugins API Reference

Courier plugins implement one of three interfaces:
:class:`~courier.interfaces.data_monitors.DataMonitorBasePlugin`,
:class:`~courier.interfaces.job_builders.JobBuilder`, or
:class:`~courier.interfaces.dispatchers.Dispatcher`. Every job builder also
nests exactly one payload plugin (`courier.interfaces.payloads.Payload`),
which describes what its jobs execute.

## Standard Data Monitors

### file_system_poller_watchdog

:class:`~courier.plugins.data_monitors.file_system_poller_watchdog.FileSystemPoller`

Watches a directory for new files and emits them to the pipeline.

## Standard Dispatchers

A dispatcher executes the *payload* a job carries. The payload is configured on
the job builder that emits the job, not on the dispatcher; a dispatcher only
declares which payload representations it can run, and how it runs them. The
options every dispatcher takes are in {doc}`dispatchers`.

### local_dispatcher

`courier.plugins.dispatchers.local_dispatcher.LocalDispatcher`

Runs a job's payload as a subprocess on the local host, one job at a time. It
runs shell, bash and python payloads, and ingests the `COURIER_METRIC:` stdout
protocol ({ref}`courier-metric-stdout`).

### slurm_dispatcher

`courier.plugins.dispatchers.slurm_dispatcher.SlurmDispatcher`

Submits a job's payload to a Slurm cluster with `sbatch`, and by default waits
for the Slurm job to finish and reports its outcome. See
{doc}`dispatchers` for its options and how each payload is submitted.

## Standard Payloads

| Plugin           | Runs                                       |
| ---------------- | ------------------------------------------ |
| `shell_payload`  | A script with `sh`, or a program.          |
| `bash_payload`   | A script with `bash`, or a program.        |
| `python_payload` | Python source with `python`, or a program. |

Configuration fields, how each payload builds its command, and the template
context are in {doc}`payloads`.

(pipeline-feedback-example)=

## Pipeline Feedback with `emit_file`

All dispatchers inherit `emit_file(file)` from the
`courier.interfaces.dispatchers.Dispatcher` base class. It publishes an output
`courier.types.datum.Datum` to the file-found exchange
(`courier.constants.FILE_FOUND_EXCHANGE`), the same fanout exchange that data
monitors use, so downstream job builders can pick it up and create new jobs.
The base class calls it, after a job has run, for each file its
`output_files` patterns find in the job's output; a custom dispatcher can call
it too.

This enables **chained pipeline workflows**: one dispatcher processes a job,
writes output files, then feeds those files back into the pipeline for a
second processing stage without any external intervention. Common patterns:

- **Multi-stage processing**: run a calibration stage, then feed calibrated
  files into a product-generation stage.
- **Fan-out reprocessing**: emit derived products as new files so several
  downstream builders can route them to different dispatchers.
- **Chained quality control**: a QC stage inspects output and emits the files
  that pass validation to the next stage.

The mechanism is identical to data monitor file emission: the file travels
through the file-found exchange, job builders consume it, and routing works
exactly as described in {doc}`../concepts/adr/0006-dispatcher-routing`.

### Without code: `output_files`

Usually no code is needed. List `output_files` patterns on the dispatcher, and
every path they match in the payload's output is emitted with the metadata the
pattern sets; see {ref}`output-files`.

The two-stage pipeline below calibrates each L1b file, prints the calibrated
file's path, and re-emits it as an L2 file for a second builder that generates
products:

```yaml
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: two-stage
  description: Calibrate L1b files, then generate products from the output.

spec:
  run:
    - watch:
        kind: data_monitor
        name: file_system_poller_watchdog
        config:
          path: /data/incoming
          metadata-tools:
            - goes18_abi

    # Stage 1 builder: raw L1b files only, never its own output
    - calibrate-builder:
        kind: job_builder
        name: filter_and_group
        config:
          files_per_job: 1
          filters:
            processing_stage: l1b
          targets:
            - calibrate
          payload:
            calibration:
              kind: payload
              name: bash_payload
              config:
                script: |
                  out=/data/l2/$(basename "{{ files[0].file }}" .nc)_cal.nc
                  calibrate "{{ files[0].file }}" "$out"
                  echo "OUTPUT: $out"

    # Stage 1 dispatcher: re-emit each printed path as an L2 file
    - calibrate:
        kind: dispatcher
        name: local_dispatcher
        config:
          output_files:
            - pattern: '^OUTPUT: (?P<file>\S+\.nc)$'
              processing_stage: l2

    # Stage 2 builder: calibrated (L2) files only
    - build-products:
        kind: job_builder
        name: filter_and_group
        config:
          files_per_job: 1
          filters:
            processing_stage: l2
          targets:
            - generate-products
          payload:
            generation:
              kind: payload
              name: bash_payload
              config:
                script: |
                  make_products "{{ files[0].file }}"

    # Stage 2 dispatcher
    - generate-products:
        kind: dispatcher
        name: local_dispatcher
```

The key points:

- Stage 1's dispatcher lists an `output_files` pattern, so each path its
  script prints after `OUTPUT:` is re-emitted as a `Datum` with
  `processing_stage: l2`.
- Every job builder receives every emitted file. Stage 1's builder filters on
  `processing_stage: l1b`, so it ignores its own L2 output; stage 2's builder
  filters on `processing_stage: l2`, so it takes only calibrated files.
- The pipeline continues naturally, with no custom wiring or external scripts.

```{warning}
**Avoid infinite loops.** Always set distinguishing metadata (for example
`processing_stage`) on emitted files, and give the producing stage's builder a
filter that excludes them. A builder with no filters picks up its own output
and processes it again, forever.
```

### From a custom dispatcher

A dispatcher that decides in code which files a job produced extends
`get_execution_log(job)`, and calls `self.emit_file()` for each of them once
the job has run:

```python
import socket
from pathlib import Path
from typing import ClassVar

from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.types.datum import Datum


class CalibrateDispatcher(LocalDispatcher):
    """Run the payload, then feed the calibrated file it wrote back in."""

    name: ClassVar[str] = "calibrate_dispatcher"
    version: ClassVar[str] = "1.0.0"

    def get_execution_log(self, job):
        logs = super().get_execution_log(job)  # runs the payload
        if all(log.return_code == 0 for log in logs):
            for source in job.files:
                self.emit_file(
                    Datum(
                        file=Path("/data/l2") / f"{Path(source.file).stem}_cal.nc",
                        hostname=socket.gethostname(),
                        source=source.source,
                        instrument=source.instrument,
                        processing_stage="l2",
                    ),
                )
        return logs
```

Publishing these files is part of the job, like publishing the `output_files`
matches and the execution logs: an exception there, a broker fault included,
fails that job (see {doc}`dispatchers`).

## Standard Job Builders

Every job builder, shipped or your own, requires a `payload` block in its
config: exactly one payload plugin, nested under `payload:`, which is what its
jobs execute. The `JobBuilder` base class checks the block and constructs the
payload when the builder is constructed, and never copies the block into the
jobs' `config`; see {doc}`payloads`.

### DummyJobBuilder

:class:`~courier.plugins.job_builders.dummy_job_builder.DummyJobBuilder`

Creates a minimal job for each file. Suitable for development and testing;
for production, use `filter_and_group` or a custom builder. Each job's
`config` is the builder's config without its `payload` and `state_sync`
blocks.

### filter_and_group

:class:`~courier.plugins.job_builders.filter_and_group.FilterAndGroupJobBuilder`

Groups files into jobs by metadata filters and optional time windows.
Jobs are emitted when the file count reaches ``files_per_job``, or when a
``window_timeout_seconds`` has elapsed and at least ``min_files`` have
accumulated (dropout path).

.. literalinclude:: ../../../src/courier/plugins/job_builders/filter_and_group.py
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
   * - ``payload``
     - payload block
     - *(required)*
     - The payload its jobs execute; see :doc:`payloads`.

#### Filter Syntax

.. _filter-syntax:

Each key-value pair in the ``filters`` dict is checked against each file
with a **two-layer lookup**:

1. **Metadata layer** — ``file.metadata.get(key)``. Keys stored in the
   metadata dict (populated from ``field_map`` entries that do not map to a
   named ``Datum`` attribute) are checked first.

2. **Attribute layer** — ``getattr(file, key, None)``. If the key is not
   found in metadata, the ``Datum`` dataclass attributes (``source``,
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

Filter configurations **must use ``Datum`` attribute names**, not legacy
field_map names. The following legacy keys are no longer recognized:

```{include} ../includes/breaking-changes.md
```

See {doc}`types` for the `Datum`/`FrozenDatum` attribute reference.

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

:class:`~courier.plugins.job_builders.metadata_router.MetadataRouterBuilder`

Routes files to different dispatchers based on file metadata (source, instrument, etc.).

Like every job builder it nests one `payload` block, in its own `config`, and
every route's jobs run that payload. A route cannot nest a payload of its own:
a route with a `payload` key is rejected when the builder is constructed, at
`courier run` startup. Use one `metadata_router` per payload instead.
