Unreleased
**********

 * *Breaking*: Scripts move from the dispatcher to a ``payload`` nested under the job builder
 * *Breaking*: Every job builder, including third-party ones, requires a ``payload`` block
 * *Breaking*: ``serial_bash``, ``parallel_bash`` and ``http_dispatcher`` removed; ``local_dispatcher`` added
 * *Breaking*: ``slurm_dispatcher`` reimplemented: ``sbatch_template`` removed, shell scripts are submitted as the batch script
 * *Breaking*: ``data-courier[http]`` and ``data-courier[all-dispatchers]`` extras removed
 * *Breaking*: Dispatcher and payload configs reject unknown and removed keys
 * *Breaking*: Payload templates are strict and render on the job builder
 * *Breaking*: Job messages carry their payload; drain queues, or upgrade builders before dispatchers
 * *Breaking*: HTTP and parallel-worker metrics removed; ``dispatcher_name`` label values change
 * *Feature*: ``shell_payload``, ``bash_payload`` and ``python_payload`` plugins (``courier.payloads``)
 * *Enhancement*: Unexecutable jobs are parked on the dead-letter queue instead of dropped
 * *Enhancement*: One bad job or template no longer ends the service process
 * *Security*: Payload ``binary`` arguments are passed as separate argv entries, never re-parsed by a shell or spliced into Python source

The full migration guide, with before-and-after configs, is
``sphinx/getting-started/upgrading.md`` ("Upgrading: Payloads Move to the Job
Builder" in the documentation). The design is ADR-0011.

Scripts move to the job builder [breaking]
==========================================

Every job builder now nests exactly one ``payload`` block holding the script
(``script:`` or ``file:``) and how to launch it. Dispatchers execute the
payload each job carries and keep only execution options (timeout, logging,
``output_files``). Before::

    - build:
        kind: job_builder
        name: filter_and_group
        config:
          files_per_job: 1
    - work:
        kind: dispatcher
        name: serial_bash
        config:
          bash_script: |
            echo "{{ files[0].file }}"

After::

    - build:
        kind: job_builder
        name: filter_and_group
        config:
          files_per_job: 1
          targets: [work]
          payload:
            echo-file:
              kind: payload
              name: bash_payload
              config:
                script: |
                  echo "{{ files[0].file }}"
    - work:
        kind: dispatcher
        name: local_dispatcher

The block is required for every job builder, and the ``JobBuilder`` base class
enforces it, so third-party builders are covered too. ``JobBuilder.__init__``
validates the block and raises ``InvalidPluginConfigError`` naming the builder
when it is missing, malformed, or nests something other than a payload, so
``courier run`` does not start; ``courier validate`` reports the same configs.
A custom builder must call ``super().__init__``, accept the ``payload`` key in
its own config checks, and must not copy the block into the config its jobs
carry. ``DummyJobBuilder`` now leaves out both ``payload`` and ``state_sync``
(which holds the Redis password) from the config its jobs carry; it used to
copy its whole config there. A builder refuses to start, and to emit a job,
until a payload is bound (``ConfigurationError``), so a job without a payload
is never published; service preflight binds it, and a test that drives a
builder directly assigns ``builder.payload`` itself.

A job carries its script once: ``PayloadSpec.script`` holds the rendered
script, and ``PayloadSpec.config`` is sent without the raw ``script`` (``file``
stays, as a path for information only). The dispatcher sets the hydrated
config's ``script`` to the rendered copy, so the template file need not exist
on the dispatcher host.

Removed and replaced plugins [breaking]
=======================================

.. list-table::
   :header-rows: 1

   * - Removed
     - Replacement
   * - ``serial_bash``
     - ``local_dispatcher`` plus a ``bash_payload`` on the job builder
   * - ``parallel_bash``
     - ``local_dispatcher`` plus a ``bash_payload``. Per-file concurrency within
       one job (``max_workers``, ``fail_fast``) is not supported: run more
       dispatcher replicas, or use smaller jobs.
   * - ``slurm_dispatcher`` (``sbatch_template``)
     - ``slurm_dispatcher`` derived from the former ``slurm_falconer``. Put
       ``#SBATCH`` lines in a shell payload's script, or use
       ``sbatch_extra_args``. Slurm output files are now
       ``<job id>-<Slurm job id>.out``/``.err``.
   * - ``http_dispatcher``
     - None.
   * - ``courier.falconers`` / ``courier.falcons`` (development builds)
     - ``courier.dispatchers`` / ``courier.payloads``

The ``data-courier[http]`` and ``data-courier[all-dispatchers]`` extras are
removed.

Stricter configuration and templates [breaking]
===============================================

``DispatcherGroupConfig`` and ``PayloadConfig`` forbid unknown keys, and each
dispatcher and payload validates with its own ``config_class``. ``courier
validate`` now checks plugin names, that each job builder nests exactly one
payload, the dispatcher and payload configs, that each payload's template
compiles, and that each payload can run on the dispatchers its builder
targets. Removed keys fail with the replacement in the message:
``bash_script``, ``max_workers``, ``fail_fast``, ``python_venv``, ``falcon``,
``falconer`` and ``sbatch_template``. A setting of any installed dispatcher
placed in a payload block is reported as belonging on the dispatcher (naming
the dispatcher when only some have it, such as ``slurm_dispatcher``'s
``partition``), and a setting of any installed payload placed in a dispatcher
block as belonging in the job builder's payload block, by ``courier
validate`` and ``courier run`` alike.

Payload templates are rendered by the job builder with strict undefined
handling: a typo or a missing key fails that job on the builder (logged at
ERROR with its files, counted in
``courier_job_builder_emit_failures_total{reason="render"}``, not published)
instead of rendering as an empty string. ``| default`` and ``is defined`` work
as usual for optional values. Template files are read, and templates parsed,
when ``courier run`` starts, so a syntax error stops the service at startup.
Only ``dispatcher``, ``script_path``, ``hostname`` and ``output_dir`` are
resolved later, on the dispatcher, and only as plain interpolations: filters,
tests and conditionals over them are errors. Job data is never re-parsed as a
template on the dispatcher.

In ``binary`` mode, the binary, ``prefix_args``, the script path and
``suffix_args`` are each rendered on their own and passed to the program as
separate arguments (``sh -c '"$@"' ...`` for shell payloads, ``subprocess.run``
of an argv for ``python_payload``). The development builds' falcons joined
them into one shell string or spliced them into Python source, so a file name
or metadata value containing a quote or ``$(...)`` could run code; argument
templates that contain quotes now also render intact.

Upgrading a running deployment [breaking]
=========================================

The job message now carries its payload. A new dispatcher parks jobs from an
old builder on ``<namespace>-JobReady-<dispatcher>-DeadLetter`` (they carry no
payload), and an old dispatcher ignores a new job's payload and runs its own
script. So never upgrade a dispatcher while jobs from an old builder can still
reach it: either stop ingest, let every ``JobReady`` queue drain and upgrade
everything together, or, in a split deployment, upgrade every builder first
and the dispatchers once the ``JobReady`` queues have drained. Jobs parked for
having no payload cannot be re-driven as they are; re-submit their files
through an upgraded builder.

Observability changes [breaking]
================================

* ``dispatcher_name`` is ``local_dispatcher`` where it was ``serial_bash`` or
  ``parallel_bash``; update queries and alerts.
* Removed: ``courier_dispatcher_http_response_codes_total``,
  ``courier_dispatcher_http_request_duration_seconds``,
  ``courier_dispatcher_parallel_workers_active``.
* Added: ``courier_payload_jobs_processed_total`` and
  ``courier_payload_job_execution_duration_seconds`` (labels ``payload_name``,
  ``payload_identifier``), and the ``payload.get_payload_from_job`` span.
* ``courier_dispatcher_jobs_processed_total`` gains ``status="unexecutable"``
  for parked jobs.
* ``courier_dispatcher_slurm_submissions_total{status="submitted"}`` counts at
  submission, and ``courier_dispatcher_slurm_jobs_pending`` covers the whole
  wait. In wait mode the payload metrics describe the Slurm job, not the
  ``sbatch`` call.
* The per-job INFO line ``Executing job: ...`` is gone. A dispatcher logs a job
  at ERROR when it fails, and the script's output with ``log_to_logger``.

Containment
===========

A render error on the builder fails only that job; the builder's consumer,
reaper, Redis-merge and hydration paths keep running. A dispatcher converts any
error while preparing or running a job into a failed job instead of exiting,
removes the job's script (now created with a random name in ``$TMPDIR``), and
parks jobs it cannot execute at all. A failure to publish a job's output files
or execution logs is not counted as a failed job: it ends the process and the
message is retried. A custom dispatcher that emits files decided
in code overrides the new ``_collect_output_files`` hook rather than calling
``emit_file`` while the job runs. Two builders that share a dispatcher no
longer have one's jobs dropped as duplicates of the other's. A payload, which
is always healthy, no longer hides a failed job builder from service health or
from the startup check.

::

    added: src/courier/interfaces/payloads.py
    added: src/courier/plugins/dispatchers/local_dispatcher.py
    added: src/courier/plugins/payloads/
    removed: src/courier/plugins/dispatchers/serial_bash.py
    removed: src/courier/plugins/dispatchers/parallel_bash.py
    removed: src/courier/plugins/dispatchers/http_dispatcher.py
    modified: src/courier/plugins/dispatchers/slurm_dispatcher.py
    added: sphinx/getting-started/upgrading.md
    added: sphinx/api-reference/dispatchers.md
    added: sphinx/api-reference/payloads.md

Version 1.0.0-alpha.29 (2026-07-29)
***********************************

 * *Breaking*: Distribution renamed from ``runcourier`` to ``data-courier``
 * *Breaking*: Plugin discovery moved from ``pluginify`` to Python entry points
 * *Breaking*: YAML plugins removed; metadata configs are Python
 * *Enhancement*: ``croniter`` is now the optional ``data-courier[cron]`` extra
 * *Breaking*: ``queues``/``plugins`` take a positional CONFIG, not ``--config``
 * *Breaking*: ``courier dashboard`` requires its CONFIG argument
 * *Enhancement*: ``courier --version``; clearer help, errors and validate output

Distribution renamed to ``data-courier``
========================================

The project is published as ``data-courier`` on PyPI. ``courier`` was already
taken, and ``runcourier`` will not receive further releases.

**Nothing changes after installation.** The import package is still ``courier``
and the CLI command is still ``courier``::

    pip install data-courier      # was: pip install runcourier

    import courier                # unchanged
    courier run config.yaml       # unchanged

The ``runcourier.dev/v1alpha1`` ``apiVersion`` in service configs is a schema
namespace, not a package name, and is **unchanged**. Existing configs need no
edits.

Optional extras are now spelled ``data-courier[s3]``, ``data-courier[cron]`` and
so on. The messages plugins emit when a dependency is missing name the correct
package -- previously they said ``pip install courier[s3]``, which installs an
unrelated project.

CLI takes a positional config everywhere
========================================

``courier queues list``, ``courier queues prune`` and ``courier plugins list``
took ``--config/-c`` while ``run``, ``validate`` and ``dashboard`` took a
positional argument, so ``courier queues list config.yaml`` failed while
``courier validate config.yaml`` worked. All of them now take the config
positionally::

    courier queues list config.yaml      # was: --config config.yaml
    courier queues prune config.yaml     # was: --config config.yaml
    courier plugins list config.yaml     # was: --config config.yaml (optional)

``courier dashboard`` now requires its config argument. It previously defaulted
to ``courier.yaml``, a filename nothing in the project creates.

``--namespace/-n`` is unchanged on the commands that have it.

Also in this release: ``courier --version``; ``courier --help`` describes the
tool rather than an internal callback; validation failures name the offending
config key instead of printing pydantic internals; ``courier validate`` reports
what it validated and the command to run next; and ``courier queues list``
gained ``--json`` to match ``courier plugins list``.

``courier.__version__`` is read from installed distribution metadata
====================================================================

It was a hand-maintained constant and had drifted to ``1.0.0-alpha.12`` while
the project shipped later versions. A release now bumps ``pyproject.toml``
only.

Version 0.2.0 (2026-05-14)
**************************

 * *Feature*: Add ``metadata`` dict to File/FrozenFile for arbitrary field_map data
 * *Enhancement*: Extend ``merge_metadata()`` with ``metadata=`` kwarg for shallow-merge
 * *Enhancement*: Filter supports two-layer lookup (metadata keys + File attributes)
 * *Breaking*: Remove legacy fallback keys (``platform``/``sensor``/``level``/``sector``)

Feature: metadata dict on File and FrozenFile
=============================================

Added a ``metadata: dict[str, Any]`` field to both :class:`~courier.types.file.File`
and :class:`~courier.types.file.FrozenFile`. This dict stores arbitrary key-value pairs
extracted from ``field_map`` entries that do not map directly to ``File`` constructor
attributes (``source``, ``instrument``, ``processing_stage``, ``domain``, ``hostname``,
``file``).

``FrozenFile.metadata`` is wrapped with :py:class:`types.MappingProxyType` via
:func:`~courier.types.file.File.freeze` for true immutability.
:func:`~courier.types.file.FrozenFile.thaw` unwraps it back to a mutable ``dict``.

``merge_metadata()`` extended with ``metadata=`` kwarg
------------------------------------------------------

:func:`~courier.types.file.File.merge_metadata` now accepts a ``metadata={...}``
keyword argument that shallow-merges into ``self.metadata``. Existing keys are
preserved; only new keys are added. This allows layering metadata from
multiple sources without overwriting previously set values.

Two-layer filter lookup
------------------------

The filter in :class:`~courier.plugins.classes.job_builders.filter_and_group.FilterAndGroupJobBuilder`
now performs a **two-layer lookup** for each filter key:

1. ``file.metadata.get(key)`` — metadata dict keys first
2. ``getattr(file, key, None)`` — ``File`` dataclass attributes second

If a key is found in neither layer, a ``WARNING`` is logged and the file is
rejected (the filter returns ``False``).

Legacy fallback keys removed [breaking]
---------------------------------------

:func:`_file_fields_from_dict` no longer recognizes the legacy fallback keys
``platform``, ``sensor``, ``level``, and ``sector``. Only the canonical
``File`` attribute names are used:

.. list-table::
   :header-rows: 1

   * - Legacy Key
     - Canonical Attribute
   * - ``platform``
     - ``source``
   * - ``sensor``
     - ``instrument``
   * - ``level``
     - ``processing_stage``
   * - ``sector``
     - ``domain``

Filter configurations that use the legacy key names must be updated.

::

     modified: src/courier/types/file.py
     modified: src/courier/plugins/classes/job_builders/filter_and_group.py
     modified: src/courier/plugins/classes/data_monitors/kafka_consumer.py
     modified: src/courier/plugins/classes/data_monitors/rabbit_mq_watcher.py
     modified: sphinx/api-reference/types.md
     modified: sphinx/api-reference/plugins.md
     modified: sphinx/getting-started/configuration.md
     modified: RELEASE.md

Version 0.1.0 (2026-04-28)
**************************

 * *Documentation*: Fix readme and pyproject.toml metadata

Documentation
===========

Fix readme and pyproject.toml metadata
-------------------------

Consolidated duplicated sections in README.md into a single clean copy. Updated pyproject.toml to include a real description (was "TODO"), extended Python version support to include 3.14, and fixed the sphinxcontrib-mermaid package name typo.


::

     modified: README.md
     modified: pyproject.toml