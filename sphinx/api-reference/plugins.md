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

## Standard Data Monitors

### file_system_poller_watchdog

:class:`~courier.plugins.monitors.file_system_poller_watchdog.FileSystemPollerWatchdog`

Watches a directory for new files and emits them to the pipeline.

### MetadataRouterBuilder

:class:`~courier.plugins.classes.job_builders.metadata_router.MetadataRouterBuilder`

Routes files to different dispatchers based on file metadata (source, instrument, etc.).
