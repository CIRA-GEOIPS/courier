# Serial Bash Dispatcher (`serial_bash`)

:class:`~courier.plugins.classes.dispatchers.serial_bash.SerialBashDispatcher`

Executes a single Jinja2-templated bash script for an entire job. One script
is rendered and executed per job — all files in the job are available in the
template as a list. Produces a single
:class:`~courier.types.execution_log.ExecutionLog` (or none if the job has no
files).

## Configuration

:class:`~courier.plugins.classes.dispatchers.serial_bash.SerialBashConfig`

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Default
     - Description
   * - ``bash_script``
     - ``str``
     - *(required)*
     - Jinja2-templated bash script. Validated for syntax at config time (fail-fast).
   * - ``timeout_seconds``
     - ``float``
     - ``3600.0``
     - Maximum script execution time in seconds. Must be greater than 0.
   * - ``log_to_logger``
     - ``bool``
     - ``False``
     - When ``True``, streams stdout/stderr to the Python logger in real time.
   * - ``log_to_file``
     - ``bool``
     - ``False``
     - When ``True``, writes script output to a log file on disk.
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
     - Regex patterns to discover output file paths from stdout (and optionally stderr). See :ref:`output-file-scanning-serial`.
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
    - process-raw:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            {% for f in files %}
            echo "Processing {{ f.file }}"
            cp {{ f.file }} /output/
            {% endfor %}
          timeout_seconds: 1800.0
```

### Logging Configuration

```{code-block} yaml
spec:
  run:
    - process-with-logs:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "Starting processing..."
            {% for f in files %}
            echo "Processing {{ f.file }}"
            {% endfor %}
          log_to_logger: true
          log_to_file: true
          log_dir: /var/log/courier
          log_only_errors: false
```

## Template Context

The following variables are available inside the Jinja2 template:

.. list-table::
   :header-rows: 1

   * - Variable
     - Description
     - Example
   * - ``files``
     - List of all :class:`~courier.types.file.FrozenFile` dicts (via :meth:`~courier.types.file.FrozenFile.to_dict`). Each dict has keys: ``file``, ``hostname``, ``source``, ``instrument``, ``processing_stage``, ``domain``, ``num_expected``, ``timestamp``.
     - ``{{ files[0].file }}``
   * - ``job``
     - Job metadata dict with keys: ``name``, ``identifier``, ``config``, ``last_modified``, ``timeout``, ``correlation_id``, ``emit_time``.
     - ``{{ job.name }}``
   * - ``config``
     - Alias for ``job.config`` (convenience access to the job's configuration dict).
     - ``{{ config.key }}``

## Error Handling

- **Config time**: Invalid Jinja2 syntax in ``bash_script`` raises
  :class:`pydantic.ValidationError` at plugin registration (fail-fast). The
  service will not start until the template is valid.
- **Render time**: Runtime template errors (e.g. accessing attributes on
  undefined variables) return
  ``ExecutionLog(return_code=-1, stderr=...)`` — the pipeline continues.
- **Execution time**: Subprocess timeouts and non-zero exit codes are captured
  as :class:`~courier.types.execution_log.ExecutionLog` entries (never
  raised). The ``return_code``, ``stdout``, and ``stderr`` fields contain the
  subprocess output.
- **Empty jobs**: If the job has no files, no script is executed and an empty
  list is returned. A warning is logged.

### Jinja2 Undefined Behavior

Uses ``jinja2.DebugUndefined`` as the undefined type. Simple undefined
variable references (e.g. ``{{ missing }}``) render as an empty string.
Attribute access on undefined variables (e.g. ``{{ missing.field }}``) raises
:exc:`jinja2.TemplateError` at render time, which is caught and returned as
an ``ExecutionLog`` with ``return_code=-1``.

## Serial vs Parallel

Use :class:`SerialBashDispatcher` when your script processes **all files
together** (one invocation, one :class:`~courier.types.execution_log.ExecutionLog`).
Use :class:`~courier.plugins.classes.dispatchers.parallel_bash.ParallelBashDispatcher`
when each file should be processed **independently** (N invocations, N
``ExecutionLog``\\s).

Key differences:

- **Serial**: All files available in a ``files`` list. Good for scripts that
  need cross-file context (e.g. generating manifests, merging data, computing
  aggregates).
- **Parallel**: Each file processed in its own subprocess. Better for
  embarrassingly parallel workloads where files don't depend on each other.

See {doc}`parallel_bash` for the parallel dispatcher documentation.

## Pipeline Feedback with ``emit_file``

.. py:method:: Dispatcher.emit_file(file: File) -> None

All dispatchers inherit :meth:`emit_file` from the
:class:`~courier.interfaces.module_based.dispatchers.Dispatcher` base class.
This method publishes an output :class:`~courier.types.file.File` to
:data:`~courier.constants.FILE_FOUND_EXCHANGE` — the same fanout exchange that
data monitors use — so downstream job builders can pick it up and create new
jobs.

This enables **chained pipeline workflows**: one dispatcher processes a job,
writes output files, then feeds those files back into the pipeline for a
second processing stage without any external intervention.

The ``serial_bash`` dispatcher can emit output files automatically via the
:ref:`output-file-scanning-serial` feature, or you can call
``self.emit_file()`` directly from a custom dispatcher subclass.

(avoid-infinite-loops)=

:::{warning}
**Avoid infinite loops.** Always set distinguishing metadata (e.g.,
``processing_stage``) on emitted files and use ``filters`` in downstream
job builders to prevent a file from being picked up by the same stage that
produced it.
:::

For a complete example with multi-stage YAML configuration, see the
:ref:`pipeline-feedback-example` section.

(output-file-scanning-serial)=

## Output File Scanning

The ``output_files`` configuration enables automatic discovery of output file
paths from the script's stdout (and optionally stderr). This is the simplest
way to feed results from a ``serial_bash`` script back into the pipeline
without writing custom dispatcher code.

### How It Works

1. After the bash script completes, the dispatcher scans the combined
   ``stdout`` (and ``stderr`` if ``scan_stderr=True``) using the configured
   regex patterns.
2. Each pattern's ``(?P<file>...)`` named group extracts file paths.
3. For each unique discovered path, a :class:`~courier.types.file.File` object
   is constructed with the hostname and any static or regex-extracted
   metadata.
4. The file is emitted via :meth:`~courier.interfaces.module_based.dispatchers.Dispatcher.emit_file`,
   making it available to downstream job builders.

### OutputFilePattern Schema

Each entry in ``output_files`` is an :class:`~courier.plugins.classes.dispatchers._output_file_pattern.OutputFilePattern`:

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Default
     - Description
   * - ``pattern``
     - ``str``
     - *(required)*
     - Regex pattern with mandatory ``(?P<file>...)`` named group. Validated at config time.
   * - ``source``
     - ``str`` | ``None``
     - ``None``
     - Source identifier applied to every matched file. Auto-lowercased.
   * - ``instrument``
     - ``str`` | ``None``
     - ``None``
     - Instrument identifier applied to every matched file. Auto-lowercased.
   * - ``processing_stage``
     - ``str`` | ``None``
     - ``None``
     - Processing stage applied to every matched file. Auto-lowercased.
   * - ``domain``
     - ``str`` | ``None``
     - ``None``
     - Domain applied to every matched file. Auto-uppercased.
   * - ``metadata``
     - ``dict[str, Any]``
     - ``{}``
     - Static metadata merged into every discovered file.

**Regex group behavior**: In addition to the mandatory ``file`` group, any
named groups matching ``File`` field names (``source``, ``instrument``,
``processing_stage``, ``domain``) override the corresponding static fields.
All other named groups are placed into the file's ``metadata`` dict, where
regex-extracted values override static ``metadata`` on key collision.

.. warning::

   **Avoid nested quantifiers** in regex patterns (e.g. ``(a+)+``,
   ``([a-z]+)*b``).  Such patterns can cause catastrophic backtracking
   (ReDoS) when matched against large stdout/stderr text.  Keep patterns
   simple and linear — use ``(?P<file>/path/.+\.nc)`` rather than
   ``(?P<file>([/\w]+)+\.nc)``.

### Output File Scanning Example

```{code-block} yaml
spec:
  run:
    - calibrate:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            {% for f in files %}
            echo "Calibrating {{ f.file }}"
            /usr/bin/calibrate --input "{{ f.file }}" --output "/data/l2/{{ f.file | basename }}"
            echo "OUTPUT: /data/l2/{{ f.file | basename }}"
            {% endfor %}
          output_files:
            - pattern: '^OUTPUT:\s+(?P<file>/data/l2/.+)$'
              processing_stage: "l2"
              source: "goes16"
              instrument: "abi"
          scan_stderr: false
```

In this example, the regex pattern ``^OUTPUT:\s+(?P<file>/data/l2/.+)$``
extracts file paths from lines prefixed with ``OUTPUT:``. Each discovered file
is emitted with ``processing_stage="l2"``, ``source="goes16"``, and
``instrument="abi"`` — making it easy for a downstream job builder to filter
for only calibrated files.

### Example with Regex-Extracted Metadata

```{code-block} yaml
config:
  bash_script: |
    #!/bin/bash
    echo "PRODUCT: goes16/abi/l2/full-disk /data/output/radiances.nc"
  output_files:
    - pattern: '^PRODUCT:\s+(?P<source>\S+)/(?P<instrument>\S+)/(?P<processing_stage>\S+)/(?P<domain>\S+)\s+(?P<file>/data/output/.+)$'
```

Here the regex extracts ``source``, ``instrument``, ``processing_stage``, and
``domain`` directly from the output line, so no static configuration is needed
for those fields.

### Preventing Infinite Loops

When using ``output_files``, the emitted files must not be picked up by the
same pipeline stage that produced them. This would cause an infinite loop
where the dispatcher processes its own output.

**To prevent this**, always set distinguishing metadata on emitted files
and use ``filters`` in downstream job builders:

```{code-block} yaml
spec:
  run:
    # Stage 1: emits files with processing_stage="l2"
    - calibrate:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "OUTPUT: /data/l2/calibrated.nc"
          output_files:
            - pattern: '^OUTPUT:\s+(?P<file>.+)$'
              processing_stage: "l2"

    # Stage 2 builder: only picks up "l2" files
    - build-products:
        kind: job_builder
        name: filter_and_group
        config:
          filters:
            processing_stage: l2
          targets:
            - generate-products

    # Stage 2: processes only calibrated files
    - generate-products:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            #!/bin/bash
            echo "Generating product from {{ files[0].file }}"
```

Key points:

- Use a unique ``processing_stage`` value that distinguishes each pipeline
  stage (e.g. ``"l2"`` for calibration output, ``"l3"`` for product output).
- Configure the downstream job builder's ``filters`` to match the specific
  stage that should consume the emitted files.
- Never configure a job builder to feed files back into the same dispatcher
  that produced them without a distinguishing metadata change.

(python-venv-serial)=

## Python Virtual Environment

When ``python_venv`` is configured, the bash script runs with the specified
virtual environment's ``bin/`` directory at the front of ``PATH`` and
``VIRTUAL_ENV`` set to the resolved absolute path.  This means commands like
``python``, ``pip``, and any venv-installed executables resolve to the
virtual environment's versions.

.. code-block:: yaml

    config:
      bash_script: |
        #!/bin/bash
        python process_data.py {{ files[0].file }}
        echo "OUTPUT: /output/result.nc"
      python_venv: /opt/venvs/data_processing
      output_files:
        - pattern: "(?P<file>OUTPUT: .*)"

.. note::

   The dispatcher sets ``PATH`` and ``VIRTUAL_ENV`` — it does **not**
   ``source activate`` the venv.  This is intentional: ``subprocess``
   cannot source shell scripts, and ``PATH`` + ``VIRTUAL_ENV`` is the
   standard, portable idiom for non-interactive venv usage.

## API Reference

.. literalinclude:: ../../../../src/courier/plugins/classes/dispatchers/serial_bash.py
   :language: python
   :start-after: class SerialBashDispatcher(Dispatcher):
   :end-before:     def __init__(
   :linenos:
