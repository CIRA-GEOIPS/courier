# Parallel Bash Dispatcher (`parallel_bash`)

:class:`~courier.plugins.classes.dispatchers.parallel_bash.ParallelBashDispatcher`

Executes a Jinja2-templated bash script independently for each file in the
job. Up to ``max_workers`` scripts run concurrently via
:class:`concurrent.futures.ThreadPoolExecutor`. Each file execution produces
its own :class:`~courier.types.execution_log.ExecutionLog`.

## Configuration

:class:`~courier.plugins.classes.dispatchers.parallel_bash.ParallelBashConfig`

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Default
     - Description
   * - ``bash_script``
     - ``str``
     - *(required)*
     - Jinja2-templated bash script rendered per file. Validated for syntax at config time (fail-fast).
   * - ``max_workers``
     - ``int``
     - ``4``
     - Maximum concurrent script executions. Must be between 1 and 64.
   * - ``timeout_seconds``
     - ``float``
     - ``3600.0``
     - Maximum execution time per file in seconds. Must be greater than 0.
   * - ``fail_fast``
     - ``bool``
     - ``False``
     - When ``True``, cancels all pending workers on the first non-zero exit code.
   * - ``log_to_logger``
     - ``bool``
     - ``False``
     - When ``True``, streams stdout/stderr to the Python logger in real time.
   * - ``log_to_file``
     - ``bool``
     - ``False``
     - When ``True``, writes each file's script output to a separate log file on disk.
   * - ``log_dir``
     - ``str``
     - ``""``
     - Directory for log files. Required when ``log_to_file=True``. Created if missing.
   * - ``log_only_errors``
     - ``bool``
     - ``False``
     - When ``True``, only logs output if the script returns non-zero or produces stderr.
   * - ``output_files``
     - ``list[OutputFilePattern]`` | ``None``
     - ``None``
     - Regex patterns to discover output file paths from each file's stdout (and optionally stderr). See :ref:`output-file-scanning-parallel`.
   * - ``python_venv``
     - ``str | None``
     - ``None``
     - Path to a Python virtual environment.  When set, the dispatcher prepends
       the venv's ``bin/`` directory to ``PATH`` and sets the ``VIRTUAL_ENV``
       environment variable before executing the bash script.  Validation
       (fail-fast) checks that the path exists, is a directory, and contains
       ``bin/python``.  Relative paths are resolved to absolute.
   * - ``scan_stderr``
     - ``bool``
     - ``False``
     - When ``True``, appends stderr to stdout before scanning for output file paths.

### Basic Example

```{code-block} yaml
spec:
  run:
    - process-files:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "Processing {{ file.file }}"
            cp "{{ file.file }}" /output/
          max_workers: 8
          timeout_seconds: 1800.0
          fail_fast: false
```

### Logging Configuration

```{code-block} yaml
spec:
  run:
    - process-with-logs:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "Processing {{ file.file }}"
            do_work --input "{{ file.file }}"
          max_workers: 4
          log_to_logger: true
          log_to_file: true
          log_dir: /var/log/courier
          log_only_errors: false
```

### Fail-Fast Example

```{code-block} yaml
spec:
  run:
    - strict-process:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            set -e
            validate "{{ file.file }}" || exit 1
            process "{{ file.file }}"
          max_workers: 4
          fail_fast: true
```

When ``fail_fast=True`` and any file's script exits non-zero, all remaining
pending workers are cancelled immediately. Already-running workers continue to
completion (they cannot be interrupted), but no new workers are submitted.

## Template Context

Each file gets its own template render with the following variables:

.. list-table::
   :header-rows: 1

   * - Variable
     - Description
     - Example
   * - ``file``
     - Current file's :class:`~courier.types.file.FrozenFile` dict (via :meth:`~courier.types.file.FrozenFile.to_dict`). Keys: ``file``, ``hostname``, ``source``, ``instrument``, ``processing_stage``, ``domain``, ``num_expected``, ``timestamp``.
     - ``{{ file.file }}``
   * - ``files``
     - List of **all** files in the job as dicts (for cross-reference or manifest generation).
     - ``{{ files[0].file }}``
   * - ``job``
     - Job metadata dict with keys: ``name``, ``identifier``, ``config``, ``last_modified``, ``timeout``, ``correlation_id``, ``emit_time``.
     - ``{{ job.name }}``
   * - ``config``
     - Alias for ``job.config`` (convenience access to the job's configuration dict).
     - ``{{ config.key }}``

Unlike :doc:`serial_bash`, the ``file`` variable refers to a single file
rather than a list. Use ``files`` (plural) when you need access to the full
file list for cross-referencing.

## Error Handling

- **Config time**: Invalid Jinja2 syntax raises
  :class:`pydantic.ValidationError` at plugin registration.
- **Render time**: Per-file template errors return
  ``ExecutionLog(return_code=-1, stderr=...)`` for that specific file —
  other files continue processing. The template is rendered independently
  for each file, so a render error in one file does not affect others.
- **Execution time**: Subprocess failures are captured as
  :class:`~courier.types.execution_log.ExecutionLog` entries with the process
  return code. Timeouts produce ``return_code=-1``.
- ``fail_fast=True`` cancels remaining pending workers on the first non-zero
  exit, but does not interrupt already-running workers.
- **Empty jobs**: If the job has no valid files (all ``file`` attributes are
  ``None``), no scripts are executed and an empty list is returned.

### Jinja2 Undefined Behavior

Uses ``jinja2.DebugUndefined``. Simple undefined references render as empty
strings; attribute access on undefined variables raises
:exc:`jinja2.TemplateError` at render time, which is caught per-file.

## Serial vs Parallel

Use :class:`~courier.plugins.classes.dispatchers.serial_bash.SerialBashDispatcher`
when your script processes **all files together** (one invocation, one
:class:`~courier.types.execution_log.ExecutionLog`). Use
:class:`ParallelBashDispatcher` when each file should be processed
**independently** (N invocations, N ``ExecutionLog``\\s).

Key differences:

- **Parallel**: Each file gets its own subprocess, enabling concurrent
  execution. Best for embarrassingly parallel workloads where files don't
  depend on each other. The template has a ``file`` variable for the current
  file and ``files`` for the full list.
- **Serial**: One script processes all files. Better for workloads requiring
  cross-file context (aggregation, manifests, merging). The template only has
  a ``files`` list.

See {doc}`serial_bash` for the serial dispatcher documentation.

## Concurrent Execution Model

Scripts are executed via :class:`concurrent.futures.ThreadPoolExecutor` with
a fixed thread pool size of ``max_workers``. Each thread runs a subprocess
(via :func:`~courier.utils.bash_executor.execute_bash_script`), so the actual
parallelism is limited by both ``max_workers`` and the system's CPU/IO
capacity.

- **Thread safety**: Each subprocess is isolated (separate process), so there
  are no shared state concerns between workers.
- **Metrics**: The active worker count is tracked via the
  ``dispatcher_parallel_workers_active`` Prometheus gauge.
- **Ordering**: Results are collected in completion order via
  :func:`concurrent.futures.as_completed`, not in submission order.

## Pipeline Feedback with ``emit_file``

The ``parallel_bash`` dispatcher supports the same :meth:`~courier.interfaces.dispatchers.Dispatcher.emit_file`
mechanism as all dispatchers. After all per-file scripts complete, the
dispatcher scans each script's output for file paths (if ``output_files`` is
configured) and emits discovered files back into the pipeline.

Because scanning happens per-file log, each emitted file is associated with
the script execution that produced it. This is transparent to the user —
duplicate file paths within a single execution log are deduplicated.

For a detailed explanation of the ``emit_file`` mechanism, see the
:ref:`pipeline-feedback-example` section in the main plugins reference.

(output-file-scanning-parallel)=

## Output File Scanning

The ``output_files`` configuration enables automatic discovery of output file
paths from each file's script stdout (and optionally stderr). This works the
same way as in :doc:`serial_bash`, but scans are performed **per-file log**
rather than once for the entire job.

### How It Works

1. After all scripts complete, each :class:`~courier.types.execution_log.ExecutionLog`
   is scanned independently.
2. The ``stdout`` (and ``stderr`` if ``scan_stderr=True``) from each
   execution is searched with the configured regex patterns.
3. **All execution logs are scanned**, including those from scripts that
   returned a non-zero exit code or timed out.  If a failed script's
   output happens to match a pattern, the file path is still emitted.
4. Each pattern's ``(?P<file>...)`` named group extracts file paths.
5. Duplicate paths within a single execution log are deduplicated.
   If two workers produce the same output path, it may be emitted once
   per worker — the downstream job builder's deduplication handles this.
6. Discovered files are emitted via :meth:`emit_file`.

### OutputFilePattern Schema

Same as :class:`~courier.plugins.classes.dispatchers._output_file_pattern.OutputFilePattern`.
See :ref:`output-file-scanning-serial` in the :doc:`serial_bash` documentation
for the full schema reference.

.. warning::

   The same :ref:`ReDoS caution about nested quantifiers <output-file-scanning-serial>`
   applies here.  Keep regex patterns simple and linear.

### Output File Scanning Example

```{code-block} yaml
spec:
  run:
    - process-files:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            result=$(process "{{ file.file }}")
            echo "RESULT: {{ file.file | basename }} -> ${result}"
          max_workers: 4
          output_files:
            - pattern: '^RESULT:\s+\S+\s+->\s+(?P<file>/data/output/.+)$'
              processing_stage: "l2"
              source: "goes16"
              instrument: "abi"
```

In this example, each file processed by ``parallel_bash`` may produce an
output file path in its stdout. Discovered paths are emitted with the
configured metadata. Deduplication is per-execution-log; the downstream
job builder provides cross-execution deduplication.

### Preventing Infinite Loops

The same caution applies as with :doc:`serial_bash`. When using
``output_files`` with parallel execution, ensure that:

- Emitted files carry distinguishing metadata (e.g. a unique
  ``processing_stage`` value).
- Downstream job builders use ``filters`` to match only the intended stage.

```{code-block} yaml
spec:
  run:
    # Stage 1: parallel processing emits l2 files
    - calibrate:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            calibrate --input "{{ file.file }}" --output "/data/l2/{{ file.file | basename }}"
            echo "CALIBRATED: /data/l2/{{ file.file | basename }}"
          max_workers: 4
          output_files:
            - pattern: '^CALIBRATED:\s+(?P<file>.+)$'
              processing_stage: "l2"

    # Stage 2 builder: only picks up l2 files
    - build-products:
        kind: job_builder
        name: filter_and_group
        config:
          filters:
            processing_stage: l2
          targets:
            - generate-products

    # Stage 2: generates products from calibrated files
    - generate-products:
        kind: dispatcher
        name: parallel_bash
        config:
          bash_script: |
            #!/bin/bash
            generate-product --input "{{ file.file }}"
          max_workers: 8
```

(python-venv-parallel)=

## Python Virtual Environment

The ``python_venv`` configuration works identically to
:ref:`serial_bash <python-venv-serial>`.  The environment dictionary is
constructed once per dispatcher invocation and passed to every worker
thread via :class:`~concurrent.futures.ThreadPoolExecutor` — each worker
receives its own copy, so this is thread-safe.

.. code-block:: yaml

    config:
      bash_script: |
        #!/bin/bash
        python process_data.py {{ file.file }}
        echo "OUTPUT: /output/{{ file.file | basename }}.nc"
      python_venv: /opt/venvs/data_processing
      max_workers: 4
      output_files:
        - pattern: "(?P<file>OUTPUT: .*)"

## API Reference

.. literalinclude:: ../../../../src/courier/plugins/classes/dispatchers/parallel_bash.py
   :language: python
   :start-after: class ParallelBashDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:
