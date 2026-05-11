"""Python class for the job_builders courier interface."""

from __future__ import annotations

import contextlib
import threading
import time
import types
from typing import TYPE_CHECKING, Any, ClassVar

from pluginify.interfaces.base import BaseClassInterface

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import (
    FatalBrokerError,
    InvalidPluginConfigError,
    TransientBrokerError,
)
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import (
    JOB_BUILDER_ACTIVE_GROUPS,
    JOB_BUILDER_EMIT_FAILURES,
    JOB_BUILDER_FILE_PROCESSING_DURATION,
    JOB_BUILDER_FILES_PER_JOB,
    JOB_BUILDER_FILES_RECEIVED,
    JOB_BUILDER_JOBS_BUILT,
    JOB_BUILDER_JOBS_DISCARDED,
    JOB_BUILDER_JOBS_EMITTED,
    collect_labeled,
)
from courier.types.file import FrozenFile
from courier.utils.decorators import log_execution, retry_with_backoff
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from courier.service import Service
    from courier.sync.job_builder_state_sync import JobBuilderStateSync
    from courier.types.job import Job, JobGroup


class JobBuilder(ServicePlugin):
    """Base data filter plugin.

    Optional HA state synchronization
    -----------------------------------
    Add a ``state_sync`` block to the plugin's ``config`` section to enable
    Redis-backed state sharing across multiple instances::

        config:
          state_sync:
            host: redis.internal
            port: 6379
            db: 1

    When enabled the builder will:

    * Refuse to start if the Redis server is unreachable.
    * Load in-progress job state from Redis on startup (crash recovery).
    * Push every job mutation to the shared Redis hash so peers stay current.
    * Use Redis SET NX to guarantee that exactly one instance emits each job.

    Requires ``pip install courier[ha]``.  Disabled by default (no
    ``state_sync`` key → no Redis dependency at runtime).
    """

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "JobBuilder"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        # pluginify registration path: instantiated with only a module (or nothing).
        if service is None or isinstance(service, types.ModuleType):
            return
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service.config)
        self.identifier = identifier or self.name
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        self.job_groups: list[JobGroup] = []
        # Thread-safe: _group_locks protects job_group.jobs dicts when
        # state_sync is enabled. Populated in start() after subclasses set
        # up job_groups. Empty dict = no locking (sync disabled).
        self._group_locks: dict[str, threading.Lock] = {}
        self.config = config or {}
        self._sync: JobBuilderStateSync | None = self._init_sync(self.config, service)
        self.targets: tuple[str, ...] = tuple(self.config.get("targets") or ())

        self._files_received = JOB_BUILDER_FILES_RECEIVED
        self._jobs_built = JOB_BUILDER_JOBS_BUILT
        self._active_job_groups = JOB_BUILDER_ACTIVE_GROUPS
        self._jobs_discarded = JOB_BUILDER_JOBS_DISCARDED
        self._file_processing_duration = JOB_BUILDER_FILE_PROCESSING_DURATION
        self._files_per_job = JOB_BUILDER_FILES_PER_JOB
        self._jobs_emitted = JOB_BUILDER_JOBS_EMITTED
        self._emit_failures = JOB_BUILDER_EMIT_FAILURES

    def call(self) -> None:
        """Plugins are driven by start()/stop(); call() is not used at runtime."""
        raise NotImplementedError("Job builder plugins are invoked via start().")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @log_execution
    def start(self) -> None:
        """Start main thread, connecting to Redis first if sync is enabled."""
        if self._state == PluginRunState.RUNNING:
            return
        if self._sync is not None:
            self._group_locks = {jg.name: threading.Lock() for jg in self.job_groups}
            self._sync.connect()  # raises StateSyncConnectionError if unreachable
            self._sync.start(self.job_groups, self._group_locks)
        self._main_thread = threading.Thread(
            target=self.handle_incoming_files,
            name=self.name,
            daemon=True,
        )
        self._state = PluginRunState.RUNNING
        self._main_thread.start()

    @log_execution
    def stop(self) -> None:
        """Stop the sync subscriber and the main thread."""
        self._state = PluginRunState.STOPPED
        if self._sync is not None:
            self._sync.stop()
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._state == PluginRunState.RUNNING

    # ------------------------------------------------------------------
    # Core file processing loop
    # ------------------------------------------------------------------

    def emit(self, job: Job, targets: Sequence[str] | None = None) -> None:
        """Fan out *job* to every dispatcher in *targets*.

        Each ``(job_id, target)`` pair is independently claimed via
        :meth:`JobBuilderStateSync.try_claim_emit` so that a crash between
        targets leaves completed targets claimed and incomplete ones free
        for resume — no duplicate executions, no silent loss.

        Parameters
        ----------
        job : Job
            Job to emit.  ``emit_time`` and ``targets`` are populated on
            a copy produced by this method before publish.
        targets : Sequence[str] or None, optional
            Dispatcher identifiers to route to.  ``None`` falls back to
            the builder's ``self.targets`` configured list.  Preflight
            guarantees at least one target is present.

        Notes
        -----
        Transient broker errors retry with backoff.  Fatal broker errors
        release the per-target claim so a restart can retry.  Partial
        fan-out is logged at ERROR with both succeeded and failed targets.
        """
        target_list: tuple[str, ...] = (
            tuple(targets) if targets is not None else self.targets
        )
        if not target_list:
            self._logger.error(
                f"emit called with no targets for job {job.identifier}; dropping",
                extra={"correlation_id": job.correlation_id},
            )
            return
        job.emit_time = time.time()
        try:
            job.targets = target_list
        except TypeError as exc:
            self._logger.exception(
                f"Failed to set job.targets for job {job.identifier}: {exc!s}",
                extra={"correlation_id": job.correlation_id},
            )
        message = str(job)
        succeeded: list[str] = []
        failed: list[tuple[str, str]] = []
        for target in target_list:
            self._emit_one(job, target, message, succeeded, failed)
        if failed:
            self._logger.error(
                f"partial fan-out for job {job.identifier}: "
                f"succeeded={succeeded} failed={failed}",
                extra={
                    "correlation_id": job.correlation_id,
                    "job_id": job.identifier,
                },
            )
        else:
            self._logger.info(
                f"Emitted job {job.identifier} to targets {list(succeeded)}",
                extra={"correlation_id": job.correlation_id},
            )

    def _emit_one(
        self,
        job: Job,
        target: str,
        message: str,
        succeeded: list[str],
        failed: list[tuple[str, str]],
    ) -> None:
        """Publish *message* to *target* with per-target claim and retry.

        Mutates *succeeded* / *failed* in place so the caller can log a
        single partial-failure line for the whole fan-out.
        """
        emit_key = f"{job.identifier}::{target}"
        if self._sync is not None and not self._sync.try_claim_emit(
            emit_key,
            job.timeout,
        ):
            self._logger.info(
                f"Job {job.identifier} target {target} already claimed; skipping",
                extra={"correlation_id": job.correlation_id},
            )
            return
        queue_name = self._resolve_target(target)
        try:
            self._publish_with_retry(queue_name, message)
        except TransientBrokerError as exc:
            self._emit_failures.labels(
                job_builder_name=self.name,
                target=target,
                reason="transient",
            ).inc()
            failed.append((target, f"transient:{exc!s}"))
        except FatalBrokerError as exc:
            self._emit_failures.labels(
                job_builder_name=self.name,
                target=target,
                reason="fatal",
            ).inc()
            failed.append((target, f"fatal:{exc!s}"))
            if self._sync is not None:
                self._sync.release_emit_claim(emit_key)
        else:
            self._jobs_emitted.labels(
                job_builder_name=self.name,
                target=target,
            ).inc()
            succeeded.append(target)

    def _resolve_target(self, target: str) -> str:
        """Resolve a dispatcher identifier to its broker queue name.

        Uses the service's :class:`TargetResolver` when available,
        falling back to :func:`courier.constants.job_ready_queue_for`
        for unit-test harnesses that construct a :class:`JobBuilder`
        without a full service.
        """
        resolver = getattr(self.parent_service, "target_resolver", None)
        if resolver is not None:
            resolved: str = resolver.resolve(target)
            return resolved
        from courier.constants import (  # noqa: PLC0415
            job_ready_queue_for,
        )

        return job_ready_queue_for(target)

    def _publish_with_retry(self, queue_name: str, message: str) -> None:
        """Publish with exponential backoff on :class:`TransientBrokerError`.

        ``FatalBrokerError`` is not retried — it propagates to the
        caller, which releases the per-target claim so a restart can
        retry.
        """

        @retry_with_backoff(
            exceptions=(TransientBrokerError,),
            max_retries=3,
            base_delay=0.5,
        )
        def _do_publish() -> None:
            self.parent_service.emit(
                queue=queue_name,
                message=message,
                confirm=True,
            )

        _do_publish()

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        self._logger.debug("Starting to handle incoming files")
        for file_string in self.parent_service.consume(FILE_FOUND_EXCHANGE):
            start_time = time.time()
            self._files_received.labels(job_builder_name=self.name).inc()
            self._logger.debug(f"Received file {file_string} from file queue")
            file = FrozenFile.from_string(str(file_string))
            for job_group in self.job_groups:
                self._logger.debug(
                    f"Processing file {file} in job group {job_group.name}",
                )
                self._process_job_group(job_group, file)
            self._file_processing_duration.labels(
                job_builder_name=self.name,
            ).observe(time.time() - start_time)
            self._active_job_groups.labels(job_builder_name=self.name).set(
                len(self.job_groups),
            )
        self._logger.error("Exiting handle_incoming_files loop unexpectedly")

    # ------------------------------------------------------------------
    # Per-group helpers (complexity-bounded)
    # ------------------------------------------------------------------

    def _targets_for_group(self, job_group: JobGroup) -> tuple[str, ...]:
        """Return the fan-out targets for *job_group*.

        Default: ``self.targets`` (applies to every group).  Routing
        builders (e.g. :class:`MetadataRouterBuilder`) override this to
        return per-route targets.
        """
        del job_group
        return self.targets

    def _process_job_group(self, job_group: JobGroup, file: FrozenFile) -> None:
        """Add a file to a group, emit ready jobs, and prune timed-out ones."""
        added, ready, updates = self._add_file_locked(job_group, file)
        if added:
            self._logger.debug(f"File added to job group {job_group.name}")
        self._push_updates(job_group.name, updates)
        targets = self._targets_for_group(job_group)
        for ready_job in ready:
            self._logger.info(f"Job {ready_job.identifier} is ready; emitting")
            self.emit(ready_job, targets)
            self._jobs_built.labels(
                status="ready",
                job_builder_name=self.name,
            ).inc()
            self._files_per_job.labels(
                job_builder_name=self.name,
            ).observe(len(ready_job.files))

        self._pop_ready_jobs(job_group, ready)

        self._cleanup_old_jobs(job_group)

    def _add_file_locked(
        self,
        job_group: JobGroup,
        file: FrozenFile,
    ) -> tuple[bool, list[Job], dict[str, Job]]:
        """Add a file under the group lock; collect ready jobs and sync updates.

        Returns
        -------
        tuple[bool, list[Job], dict[str, Job]]
            ``(added, ready_jobs, updates)`` where *updates* maps job IDs
            to the modified ``Job`` objects that should be pushed to Redis.
        """
        lock = self._group_locks.get(job_group.name)
        updates: dict[str, Job] = {}
        ready: list[Job] = []
        with lock if lock is not None else contextlib.nullcontext():
            added = job_group.add_file(file)
            if added:
                ready = job_group.ready_jobs()
                updates = self._collect_sync_updates(job_group, file)
        return added, ready, updates

    def _collect_sync_updates(
        self,
        job_group: JobGroup,
        file: FrozenFile,
    ) -> dict[str, Job]:
        """Snapshot jobs affected by the last add_file call for Redis push.

        Must be called while the group lock is held.
        """
        if self._sync is None:
            return {}
        return {
            jid: job_group.jobs[jid]
            for jid in job_group.get_job_ids_from_file(file)
            if jid in job_group.jobs
        }

    def _push_updates(self, group_name: str, updates: dict[str, Job]) -> None:
        """Push a batch of job updates to Redis (no-op when sync is disabled)."""
        if self._sync is None:
            return
        for jid, job in updates.items():
            self._sync.push_job_update(group_name, jid, job)

    def _cleanup_old_jobs(self, job_group: JobGroup) -> None:
        """Remove timed-out jobs from the group and sync the deletions."""
        deletions = self._collect_and_delete_old_jobs(job_group)
        self._push_deletions(job_group.name, deletions)

    def _collect_and_delete_old_jobs(self, job_group: JobGroup) -> list[str]:
        """Delete timed-out jobs under the group lock; return their IDs."""
        lock = self._group_locks.get(job_group.name)
        with lock if lock is not None else contextlib.nullcontext():
            old_ids = [jid for jid, job in job_group.jobs.items() if job.is_old()]
            for job_id in old_ids:
                self._log_discard(job_id)
                del job_group.jobs[job_id]
        return old_ids

    def _log_discard(self, job_id: str) -> None:
        """Log and count a discarded job."""
        self._logger.info(f"Discarding old job {job_id}")
        self._jobs_discarded.labels(job_builder_name=self.name).inc()
        self._jobs_built.labels(
            status="old",
            job_builder_name=self.name,
        ).inc()

    def _push_deletions(self, group_name: str, deletions: list[str]) -> None:
        """Notify peers of deleted jobs (no-op when sync is disabled)."""
        if self._sync is None:
            return
        for job_id in deletions:
            self._sync.push_job_deletion(group_name, job_id)

    def _pop_ready_jobs(
        self,
        job_group: JobGroup,
        ready_jobs: list[Job],
    ) -> None:
        """Remove emitted ready jobs from the group and sync the deletions.

        Parameters
        ----------
        job_group : JobGroup
            The group to remove jobs from.
        ready_jobs : list[Job]
            Ready jobs that have been emitted and should be removed.
        """
        if not ready_jobs:
            return
        lock = self._group_locks.get(job_group.name)
        deletions: list[str] = []
        with lock if lock is not None else contextlib.nullcontext():
            for job in ready_jobs:
                if job.identifier in job_group.jobs:
                    del job_group.jobs[job.identifier]
                    deletions.append(job.identifier)
        self._push_deletions(job_group.name, deletions)

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def get_metrics(self) -> dict[str, Any]:
        """Return plugin-specific metrics."""
        return {
            **collect_labeled(
                JOB_BUILDER_FILES_RECEIVED,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(JOB_BUILDER_JOBS_BUILT, "job_builder_name", self.name),
            **collect_labeled(
                JOB_BUILDER_ACTIVE_GROUPS,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(
                JOB_BUILDER_JOBS_DISCARDED,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(
                JOB_BUILDER_FILE_PROCESSING_DURATION,
                "job_builder_name",
                self.name,
            ),
            **collect_labeled(
                JOB_BUILDER_FILES_PER_JOB,
                "job_builder_name",
                self.name,
            ),
        }

    # ------------------------------------------------------------------
    # Config parsing
    # ------------------------------------------------------------------

    def _init_sync(
        self,
        config: dict[str, Any],
        service: Service,
    ) -> JobBuilderStateSync | None:
        """Parse ``state_sync`` config and return a sync object, or None.

        Raises
        ------
        InvalidPluginConfigError
            If ``state_sync`` is present but the ``redis`` package is not
            installed (``pip install courier[ha]``).
        pydantic.ValidationError
            If the ``state_sync`` config values are invalid.
        """
        raw = config.get("state_sync")
        if raw is None:
            return None
        try:
            from courier.schema.v1alpha1.sync_config import (  # noqa: PLC0415
                RedisStateSyncConfig,
            )
            from courier.sync.job_builder_state_sync import (  # noqa: PLC0415
                JobBuilderStateSync,
            )
        except ImportError as exc:
            raise InvalidPluginConfigError(
                "state_sync requires the redis package: pip install courier[ha]",
            ) from exc
        sync_config = RedisStateSyncConfig.model_validate(raw)
        return JobBuilderStateSync(
            config=sync_config,
            namespace=service.config.namespace,
            builder_name=self.name,
        )


class JobBuilderInterface(BaseClassInterface):
    """Interface for courier job builder plugins."""

    name: ClassVar[str] = "job_builders"
    plugin_class: ClassVar[type] = JobBuilder
    required_args: ClassVar[dict[str, list[str]]] = {"standard": []}
    required_kwargs: ClassVar[dict[str, list[str]]] = {"standard": []}
    # ignoring odd capitalization to match Kubernetes apiVersion conventions
    apiVersion: ClassVar[str] = "runcourier.dev/v1alpha1"  # noqa: N815


job_builders = JobBuilderInterface()
