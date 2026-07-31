"""Python class for the job_builders courier interface."""

from __future__ import annotations

import contextlib
import threading
import time
from typing import TYPE_CHECKING, Any, ClassVar

from opentelemetry.trace import Status, StatusCode, get_current_span

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import (
    FatalBrokerError,
    InvalidPluginConfigError,
    TransientBrokerError,
)
from courier.interfaces.discovery import (
    ENTRY_POINT_PREFIX,
    ClassPluginRegistry,
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
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_FILE_PATH,
    ATTR_JOB_GROUP_NAME,
    ATTR_JOB_ID,
    ATTR_JOB_NAME,
    ATTR_PLUGIN_NAME,
    ATTR_PLUGIN_VERSION,
    ATTR_TARGET,
    get_tracer,
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

    Requires ``pip install data-courier[ha]``.  Disabled by default (no
    ``state_sync`` key → no Redis dependency at runtime).
    """

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "JobBuilder"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        self.parent_service = service
        self._logger = get_logger("plugin", self.name, service.config)
        self.identifier = identifier or self.name
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        # Per-instance shutdown signal handed to Service.consume() so the
        # broker loop returns on stop(). See Dispatcher.__init__ for why this
        # is per-instance rather than service-wide.
        self._stop_event = threading.Event()
        # Set once this builder's queue is bound to the file-found fanout
        # exchange. Producers must not start before this: a fanout exchange
        # drops messages published while nothing is bound, so files emitted
        # during the startup window would be lost with no error anywhere.
        self._subscribed = threading.Event()
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
        self._stop_event.clear()
        self._subscribed.clear()
        # daemon=True is a backstop only; stop() sets _stop_event and joins.
        self._main_thread = threading.Thread(
            target=self._run_handle_incoming_files,
            name=self.name,
            daemon=True,
        )
        self._state = PluginRunState.RUNNING
        self._main_thread.start()

    @log_execution
    def stop(self) -> None:
        """Stop the sync subscriber and the main thread."""
        self._state = PluginRunState.STOPPED
        self._stop_event.set()
        if self._sync is not None:
            self._sync.stop()
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=5)

    def is_healthy(self) -> bool:
        """Check if plugin is healthy."""
        return self._state == PluginRunState.RUNNING

    def wait_until_subscribed(self, timeout: float) -> bool:
        """Block until this builder is bound to the file-found exchange.

        Returns
        -------
        bool
            ``True`` if the subscription completed within *timeout*.
        """
        return self._subscribed.wait(timeout=timeout)

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
        job.targets = target_list
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
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "job_builder.emit_one",
            attributes={ATTR_TARGET: target},
        ):
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
                    job_builder_identifier=self.identifier,
                    target=target,
                    reason="transient",
                ).inc()
                failed.append((target, f"transient:{exc!s}"))
            except FatalBrokerError as exc:
                self._emit_failures.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                    target=target,
                    reason="fatal",
                ).inc()
                failed.append((target, f"fatal:{exc!s}"))
                if self._sync is not None:
                    self._sync.release_emit_claim(emit_key)
            else:
                self._jobs_emitted.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
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

    def _run_handle_incoming_files(self) -> None:
        """Wrapper that exits the process on any unhandled exception."""
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "job_builder.handle_incoming_files",
            attributes={
                ATTR_PLUGIN_NAME: self.name,
                ATTR_PLUGIN_VERSION: self.version,
            },
        ) as span:
            try:
                self.handle_incoming_files()
            except Exception:
                import os
                import traceback
                traceback.print_exc()
                span.set_status(Status(StatusCode.ERROR))
                self._logger.critical(
                    "Fatal error in job builder %s: exiting", self.name,
                )
                os._exit(1)

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        self._logger.debug("Starting to handle incoming files")
        tracer = get_tracer(__name__)
        for file_string, parent_ctx in self.parent_service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=self._stop_event,
            on_subscribed=self._subscribed.set,
        ):
            start_time = time.time()
            file = FrozenFile.from_string(str(file_string))
            with tracer.start_as_current_span(
                "job_builder.build_job",
                context=parent_ctx,
                attributes={
                    ATTR_FILE_PATH: str(file.file) if file.file else "",
                },
            ):
                self._files_received.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).inc()
                self._logger.debug(f"Received file {file_string} from file queue")
                for job_group in self.job_groups:
                    self._logger.debug(
                        f"Processing file {file} in job group {job_group.name}",
                    )
                    self._process_job_group(job_group, file)
                self._file_processing_duration.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).observe(time.time() - start_time)
                self._active_job_groups.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).set(
                    len(self.job_groups),
                )
        if self._stop_event.is_set():
            self._logger.info("handle_incoming_files loop exited on shutdown")
        else:
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
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "job_builder.process_job_group",
            attributes={ATTR_JOB_GROUP_NAME: job_group.name},
        ):
            added, ready, updates = self._add_file_locked(job_group, file)
            if added:
                self._logger.debug(f"File added to job group {job_group.name}")
            self._push_updates(job_group.name, updates)
            # `ready` was already removed from the group under the same lock
            # that added the file, so these jobs are exclusively ours to emit.
            self._push_deletions(job_group.name, [j.identifier for j in ready])
            targets = self._targets_for_group(job_group)
            for ready_job in ready:
                self._logger.info(f"Job {ready_job.identifier} is ready; emitting")
                with tracer.start_as_current_span(
                    "job_builder.emit_job",
                    attributes={
                        ATTR_JOB_ID: ready_job.identifier,
                        ATTR_JOB_NAME: ready_job.name or "",
                        ATTR_CORRELATION_ID: ready_job.correlation_id,
                    },
                ):
                    self.emit(ready_job, targets)
                    get_current_span().add_event(
                        "job.ready",
                        attributes={
                            ATTR_JOB_ID: ready_job.identifier,
                            ATTR_CORRELATION_ID: ready_job.correlation_id,
                        },
                    )
                    get_current_span().add_event(
                        "job.emitted",
                        attributes={
                            ATTR_JOB_ID: ready_job.identifier,
                            ATTR_CORRELATION_ID: ready_job.correlation_id,
                        },
                    )
                self._jobs_built.labels(
                    status="ready",
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).inc()
                self._files_per_job.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).observe(len(ready_job.files))

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
                updates = self._collect_sync_updates(job_group, file)
                # Claim ready jobs *inside* the lock. Previously they were
                # merely listed here and not removed until after emit(), so a
                # timeout reaper running concurrently could pop and emit the
                # same job in that window -- the dispatcher then saw the job
                # twice and its dedupe LRU decided which copy to drop.
                ready = self._claim_ready_jobs(job_group)
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
        self._jobs_discarded.labels(
            job_builder_name=self.name,
            job_builder_identifier=self.identifier,
        ).inc()
        self._jobs_built.labels(
            status="old",
            job_builder_name=self.name,
            job_builder_identifier=self.identifier,
        ).inc()

    def _push_deletions(self, group_name: str, deletions: list[str]) -> None:
        """Notify peers of deleted jobs (no-op when sync is disabled)."""
        if self._sync is None:
            return
        for job_id in deletions:
            self._sync.push_job_deletion(group_name, job_id)

    def _claim_ready_jobs(self, job_group: JobGroup) -> list[Job]:
        """Remove and return every ready job, taking ownership of each.

        Caller must hold the group lock. Removing under the same lock that
        found them is what makes emission exclusive: any concurrent reaper
        sees an empty group rather than a job already in flight.
        """
        claimed: list[Job] = []
        for job_id in [jid for jid, job in job_group.jobs.items() if job.ready()]:
            claimed.append(job_group.jobs.pop(job_id))
            job_group._record_job_emitted(job_id)
        return claimed

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
                    job_group._record_job_emitted(job.identifier)
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
            installed (``pip install data-courier[ha]``).
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
                "state_sync requires the redis package: pip install data-courier[ha]",
            ) from exc
        sync_config = RedisStateSyncConfig.model_validate(raw)
        return JobBuilderStateSync(
            config=sync_config,
            namespace=service.config.namespace,
            builder_name=self.name,
        )


#: Registry of job builder plugins, read from the ``courier.job_builders``
#: entry-point group. Hands back classes; ``PluginManager`` constructs them.
job_builders = ClassPluginRegistry(
    name="job_builders",
    group=f"{ENTRY_POINT_PREFIX}.job_builders",
    expected_base=JobBuilder,
)
