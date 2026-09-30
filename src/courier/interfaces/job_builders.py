"""Python class for the job_builders courier interface."""

from __future__ import annotations

import contextlib
import enum
import os
import threading
import time
import traceback
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, cast

from opentelemetry.trace import Status, StatusCode, get_current_span
from pydantic import ValidationError

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import (
    CourierError,
    FatalBrokerError,
    InvalidPluginConfigError,
    TransientBrokerError,
)
from courier.interfaces.discovery import (
    ENTRY_POINT_PREFIX,
    ClassPluginRegistry,
)
from courier.interfaces.payloads import payloads
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
    JOB_BUILDER_MALFORMED_MESSAGES,
    collect_labeled,
)
from courier.schema.v1alpha1.service_config import MicroserviceModel
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_FILE_PATH,
    ATTR_FILE_SOURCE,
    ATTR_JOB_GROUP_NAME,
    ATTR_JOB_ID,
    ATTR_JOB_NAME,
    ATTR_PLUGIN_NAME,
    ATTR_PLUGIN_VERSION,
    ATTR_TARGET,
    get_tracer,
)
from courier.types.datum import FrozenDatum
from courier.types.payload import PayloadSpec
from courier.utils.decorators import log_execution, retry_with_backoff
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from courier.interfaces.payloads import Payload
    from courier.service import Service
    from courier.sync.job_builder_state_sync import JobBuilderStateSync
    from courier.types.job import Job, JobGroup


#: Bytes of a malformed message body echoed into the error log.
_MALFORMED_BODY_PREVIEW = 512
#: File paths of a job that failed to render listed in the error log.
_RENDER_FAILURE_FILE_PREVIEW = 10

#: Config key under which every job builder nests its payload plugin.
PAYLOAD_KEY = "payload"

#: The smallest valid ``payload`` block, quoted by every error about one.
_PAYLOAD_BLOCK_EXAMPLE = """\
  payload:
    my-payload:
      kind: payload
      name: bash_payload
      config:
        script: echo {{ files[0].file }}"""


def block_error_location(
    raw: Mapping[str, Any],
    loc: tuple[int | str, ...],
) -> tuple[int | str, ...]:
    """Map a :class:`MicroserviceModel` error location onto the keys written.

    The model reads the short form ``{<identifier>: {kind, name, config}}`` as
    the fields ``identifier`` and ``spec``, so pydantic reports ``spec.kind``
    where the YAML says ``<identifier>.kind``; the canonical form is kept.
    """
    if "identifier" in raw or "spec" in raw or len(raw) != 1:
        return loc
    if loc and loc[0] in {"identifier", "spec"}:
        return (str(next(iter(raw))), *loc[1:])
    return loc


def parse_payload_block(
    builder: str,
    config: Mapping[str, Any] | None,
) -> MicroserviceModel:
    """Return the validated ``payload`` block of a job builder's config.

    Parameters
    ----------
    builder : str
        The builder's identifier, for the error message.
    config : Mapping[str, Any] or None
        The builder's config.

    Returns
    -------
    MicroserviceModel
        The nested payload plugin: its identifier, kind, name and config.

    Raises
    ------
    InvalidPluginConfigError
        If the block is missing, is not one valid plugin mapping, nests a
        plugin whose kind is not ``payload``, or gives that plugin settings
        that are not a mapping.
    """
    # Imported here: courier.cli.plugins imports every interface, this one too.
    from courier.cli.plugins import normalize_kind  # noqa: PLC0415

    def _error(problem: str) -> InvalidPluginConfigError:
        return InvalidPluginConfigError(
            f"Job builder {builder!r} {problem}. Every job builder needs a "
            "payload block: its config nests exactly one payload plugin under "
            f"`{PAYLOAD_KEY}:`, which is what its jobs execute. For example:\n"
            f"{_PAYLOAD_BLOCK_EXAMPLE}",
        )

    raw = config.get(PAYLOAD_KEY) if isinstance(config, Mapping) else None
    if raw is None:
        raise _error(f"has no '{PAYLOAD_KEY}' block")
    malformed = f"has a malformed '{PAYLOAD_KEY}' block"
    if not isinstance(raw, Mapping):
        raise _error(f"{malformed} (should be a mapping, not {type(raw).__name__})")
    if "identifier" not in raw and "spec" not in raw and len(raw) != 1:
        raise _error(
            f"{malformed} (it takes exactly one payload plugin; found {len(raw)})",
        )
    raw = dict(raw)  # a copy: the model adapts the short form of a dict only
    try:
        block = MicroserviceModel.model_validate(raw)
    except ValidationError as exc:
        parts = []
        for error in exc.errors(include_url=False):
            where = ".".join(map(str, block_error_location(raw, error["loc"])))
            message = error["msg"].removeprefix("Value error, ")
            parts.append(f"{where}: {message}" if where else message)
        raise _error(f"{malformed} ({'; '.join(parts)})") from exc
    except TypeError as exc:  # a plugin that maps to something not a mapping
        raise _error(f"{malformed} ({exc})") from exc
    if normalize_kind(block.spec.kind) != "payloads":
        raise _error(
            f"nests {block.identifier!r} of kind {block.spec.kind!r} under "
            f"'{PAYLOAD_KEY}', but only kind 'payload' can be nested there",
        )
    settings = block.spec.config
    if settings is not None and not isinstance(settings, Mapping):
        where = ".".join(map(str, block_error_location(raw, ("spec", "config"))))
        raise _error(
            f"{malformed} ({where}: should be a mapping of settings, not "
            f"{type(settings).__name__})",
        )
    return block


def _rendered_spec(payload: Payload, job: Job, builder: JobBuilder) -> PayloadSpec:
    """Render *payload* onto *job*, refusing a ``to_job_spec`` with no spec."""
    spec = payload.to_job_spec(job, builder=builder)
    if not isinstance(spec, PayloadSpec):
        raise CourierError(
            f"payload {payload.identifier!r} rendered no PayloadSpec "
            f"(its to_job_spec returned {type(spec).__name__})",
        )
    return spec


class _EmitOutcome(enum.Enum):
    """What :meth:`JobBuilder._emit_job` did with a job."""

    #: At least one target took the job.
    PUBLISHED = "published"
    #: Never publishable: no targets, or the payload failed to render.
    DROPPED = "dropped"
    #: Every target the builder claimed failed with a broker error.
    FAILED = "failed"
    #: Every target was already claimed by a peer; the files go back.
    UNCLAIMED = "unclaimed"


class JobBuilder(ServicePlugin):
    """Base data filter plugin.

    Required payload
    ----------------
    Every job builder nests exactly one payload plugin under ``payload`` in
    its ``config`` (see :func:`parse_payload_block`); it is what the builder's
    jobs execute. The base class validates the block and constructs the
    plugin as :attr:`payload`, so a subclass that calls ``super().__init__``
    cannot be built without one, and every job it emits carries it.

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
    #: Name of the per-file span. Subclasses may override it.
    _file_span_name: ClassVar[str] = "job_builder.build_job"
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
        self.config = config or {}
        # Before anything else is set up: a builder without a payload cannot run.
        block = parse_payload_block(self.identifier, self.config)
        payload_cls = cast("type[Payload]", payloads.get_plugin(block.spec.name))
        #: The payload plugin rendered onto every job this builder emits.
        self.payload: Payload = payload_cls(
            service,
            block.spec.config,
            block.identifier,
        )
        self._state = PluginRunState.STOPPED
        self._main_thread: threading.Thread | None = None
        # Per-instance shutdown signal handed to Service.consume() so the
        # broker loop returns on stop(). See Dispatcher.__init__ for why this
        # is per-instance rather than service-wide.
        self._stop_event = threading.Event()
        # Set once this builder's durable queue is declared and bound to the
        # file-found exchange. PluginManager waits on it before starting
        # producers so the first files move promptly. The queue is durable and
        # predeclared during preflight, so a wait that times out loses nothing.
        self._subscribed = threading.Event()
        self.job_groups: list[JobGroup] = []
        # Thread-safe: _group_locks protects job_group.jobs dicts when
        # state_sync is enabled. Populated in start() after subclasses set
        # up job_groups. Empty dict = no locking (sync disabled).
        self._group_locks: dict[str, threading.Lock] = {}
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
        self._metric_labels = {
            "job_builder_name": self.name,
            "job_builder_identifier": self.identifier,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @log_execution
    def start(self) -> None:
        """Start main thread, connecting to Redis first if sync is enabled."""
        if self._state == PluginRunState.RUNNING:
            return
        # Locks are populated whether or not state sync is configured: the
        # timeout reaper mutates the same groups from its own thread either
        # way. setdefault keeps a dict a subclass built in __init__, whose
        # lock objects the reaper may already hold.
        for group in self.job_groups:
            self._group_locks.setdefault(group.name, threading.Lock())
        self._check_replication_safety()
        if self._sync is not None:
            self._sync.connect()  # raises StateSyncConnectionError if unreachable
            self._sync.set_merge_callback(self._emit_ready_jobs)
            self._sync.start(self.job_groups, self._group_locks)
            # Hydration merges the shared hash into the local groups without
            # firing the merge callback. A job already complete when this
            # replica starts would otherwise wait for an unrelated file in its
            # group, or for job.timeout to discard it. That state is reached
            # when a peer died between push_job_update and push_job_deletion.
            for group in self.job_groups:
                self._emit_ready_jobs(group, reason="was complete in shared state")
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

    @property
    def accumulates(self) -> bool:
        """Whether any group gathers more than one file into a job.

        Read from the configured group capacity, so it tracks what the builder
        does. A builder that emits one job per file is safe to replicate
        without shared state. A builder that gathers files is not, because
        replicas see different files.
        """
        return any(
            int(getattr(group.config, "files_per_job", 1) or 1) != 1
            for group in self.job_groups
        )

    def _check_replication_safety(self) -> None:
        """Refuse to start if replicating this builder would split its jobs.

        See :class:`~courier.errors.UnsafeReplicationError` for why an
        accumulating builder cannot be replicated without shared state.

        The peer count is a snapshot. A replica that has started but not yet
        bound its queue is invisible, so a builder with no observable peer
        warns and starts; refusing would block ordinary single-replica
        deployments.

        Raises
        ------
        UnsafeReplicationError
            When another consumer is already attached to this builder's queue
            and nothing would let the two reassemble a job.
        """
        from courier.errors import UnsafeReplicationError  # noqa: PLC0415

        if self._sync is not None or not self.accumulates:
            return
        peers = self._peer_consumer_count()
        if peers > 0:
            raise UnsafeReplicationError(self.identifier, peers)
        self._logger.warning(
            "Job builder %r groups files into jobs and has no state_sync "
            "block. Running a second replica of this identifier would "
            "split every job across replicas and emit them short. No peer "
            "is attached right now, so startup continues.",
            self.identifier,
        )

    def _peer_consumer_count(self) -> int:
        """Return consumers already on this builder's queue, or 0 if unknown."""
        from courier.constants import file_found_queue_for  # noqa: PLC0415

        manager = getattr(self.parent_service, "_broker_manager", None)
        counter = getattr(manager, "consumer_count", None)
        namer = getattr(manager, "get_queue_name", None)
        if counter is None or namer is None:
            return 0
        try:
            count = counter(namer(file_found_queue_for(self.identifier)))
        except Exception:  # a stub service in a unit test, or an odd transport
            return 0
        return int(count or 0)

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

    def emit(self, job: Job, targets: Sequence[str] | None = None) -> bool:
        """Fan out *job* to every dispatcher in *targets*.

        Each ``(job_id, target)`` pair is independently claimed via
        :meth:`JobBuilderStateSync.try_claim_emit` so that a crash between
        targets leaves completed targets claimed and incomplete ones free
        for resume — no duplicate executions, no silent loss.

        Parameters
        ----------
        job : Job
            Job to emit.  ``emit_time``, ``targets`` and ``payload`` are
            populated on the job before publish.
        targets : Sequence[str] or None, optional
            Dispatcher identifiers to route to.  ``None`` falls back to
            the builder's ``self.targets`` configured list.  Preflight
            guarantees at least one target is present.

        Returns
        -------
        bool
            ``False`` when every target was skipped because a peer already
            holds its claim, so the job reached no broker.  The caller has
            already removed the job from its group; a ``False`` it ignores
            discards the job's files silently.  See
            :meth:`_return_files_to_group`.  A job whose payload fails to
            render returns ``True``: it is dropped, as its files would fail
            the same render again.

        Notes
        -----
        Transient broker errors retry with backoff.  Fatal broker errors
        release the per-target claim so a restart can retry.  Partial
        fan-out is logged at ERROR with both succeeded and failed targets.
        A render failure is logged at ERROR, counted under
        ``reason="render"`` and never raised, so every caller survives it.
        """
        return self._emit_job(job, targets) is not _EmitOutcome.UNCLAIMED

    def _emit_job(
        self,
        job: Job,
        targets: Sequence[str] | None = None,
    ) -> _EmitOutcome:
        """Fan out *job* as :meth:`emit` does, reporting what happened to it."""
        target_list: tuple[str, ...] = (
            tuple(targets) if targets is not None else self.targets
        )
        if not target_list:
            self._logger.error(
                f"emit called with no targets for job {job.identifier}; dropping",
                extra={"correlation_id": job.correlation_id},
            )
            # Not recoverable by re-adding the files: with no target they
            # would be dropped again on every pass.
            return _EmitOutcome.DROPPED
        job.emit_time = time.time()
        job.targets = target_list
        message = self._render_message(job, target_list)
        if message is None:
            return _EmitOutcome.DROPPED
        succeeded: list[str] = []
        failed: list[tuple[str, str]] = []
        claimed = sum(
            self._emit_one(job, target, message, succeeded, failed)
            for target in target_list
        )
        if failed:
            self._logger.error(
                f"partial fan-out for job {job.identifier}: "
                f"succeeded={succeeded} failed={failed}",
                extra={
                    "correlation_id": job.correlation_id,
                    "job_id": job.identifier,
                },
            )
            return _EmitOutcome.PUBLISHED if succeeded else _EmitOutcome.FAILED
        if claimed == len(target_list):
            # Every target was a peer's. WARNING because the caller has to act
            # on it; at INFO it read like an ordinary dedup while the job's
            # files were dropped.
            self._logger.warning(
                f"Job {job.identifier} was claimed by a peer for every target "
                f"{list(target_list)}; returning its {len(job.files)} file(s) "
                f"to the group rather than dropping them",
                extra={"correlation_id": job.correlation_id},
            )
            return _EmitOutcome.UNCLAIMED
        self._logger.info(
            f"Emitted job {job.identifier} to targets {list(succeeded)}",
            extra={"correlation_id": job.correlation_id},
        )
        return _EmitOutcome.PUBLISHED

    def _render_message(self, job: Job, target_list: tuple[str, ...]) -> str | None:
        """Render the payload onto *job* (pass one) and serialize it.

        Rendering depends on the job's data, so it can fail for one job and
        not the next. Any failure, including a ``to_job_spec`` that returns no
        :class:`PayloadSpec`, is logged at ERROR, counted once per target
        under ``reason="render"``, and returned as ``None``.
        """
        try:
            job.payload = _rendered_spec(self.payload, job, self)
            return str(job)
        except Exception as exc:  # render boundary: fail this job, not the builder
            span = get_current_span()
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(exc)
            files = sorted(str(f.file) for f in job.files)
            shown = files[:_RENDER_FAILURE_FILE_PREVIEW]
            more = len(files) - len(shown)
            self._logger.exception(
                f"Dropping job {job.identifier}: it failed to render (payload "
                f"{self.payload.identifier!r}), so nothing was published to "
                f"{list(target_list)}. Files ({len(files)}): {shown}"
                f"{f' and {more} more' if more else ''}",
                extra={"correlation_id": job.correlation_id},
            )
            for target in target_list:
                self._emit_failures.labels(
                    target=target,
                    reason="render",
                    **self._metric_labels,
                ).inc()
            return None

    def _emit_one(
        self,
        job: Job,
        target: str,
        message: str,
        succeeded: list[str],
        failed: list[tuple[str, str]],
    ) -> bool:
        """Publish *message* to *target* with per-target claim and retry.

        Mutates *succeeded* / *failed* in place so the caller can log a
        single partial-failure line for the whole fan-out.

        Returns
        -------
        bool
            ``True`` when a peer already holds the claim and nothing was
            published.  The caller counts these to spot a job no target took.
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
                return True
            queue_name = self._resolve_target(target)
            try:
                self._publish_with_retry(queue_name, message)
            except TransientBrokerError as exc:
                self._emit_failures.labels(
                    target=target,
                    reason="transient",
                    **self._metric_labels,
                ).inc()
                failed.append((target, f"transient:{exc!s}"))
            except FatalBrokerError as exc:
                self._emit_failures.labels(
                    target=target,
                    reason="fatal",
                    **self._metric_labels,
                ).inc()
                failed.append((target, f"fatal:{exc!s}"))
                if self._sync is not None:
                    self._sync.release_emit_claim(emit_key)
            else:
                self._jobs_emitted.labels(
                    target=target,
                    **self._metric_labels,
                ).inc()
                succeeded.append(target)
        return False

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
            self.parent_service.emit(queue=queue_name, message=message)

        _do_publish()

    def _run_handle_incoming_files(self) -> None:
        """Exit the process on any unhandled exception."""
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
                traceback.print_exc()
                span.set_status(Status(StatusCode.ERROR))
                self._logger.critical(
                    "Fatal error in job builder %s: exiting",
                    self.name,
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
            subscriber=self.identifier,
        ):
            start_time = time.time()
            file = self._parse_file_message(str(file_string))
            if file is None:
                # Dropped: returning to the consume loop acknowledges it.
                continue
            with tracer.start_as_current_span(
                self._file_span_name,
                context=parent_ctx,
                attributes={
                    ATTR_FILE_PATH: str(file.file) if file.file else "",
                    # Dashboards group router spans by this attribute.
                    ATTR_FILE_SOURCE: file.source or "",
                },
            ):
                self._files_received.labels(**self._metric_labels).inc()
                self._logger.debug(f"Received file {file_string} from file queue")
                self._dispatch_file(file)
                self._file_processing_duration.labels(
                    **self._metric_labels,
                ).observe(time.time() - start_time)
                self._active_job_groups.labels(**self._metric_labels).set(
                    len(self.job_groups),
                )
        if self._stop_event.is_set():
            self._logger.info("handle_incoming_files loop exited on shutdown")
        else:
            self._logger.error("Exiting handle_incoming_files loop unexpectedly")

    def _dispatch_file(self, file: FrozenDatum) -> None:
        """Hand *file* to every job group; routing builders override this hook."""
        for job_group in self.job_groups:
            self._logger.debug(
                f"Processing file {file} in job group {job_group.name}",
            )
            self._process_job_group(job_group, file)

    def _parse_file_message(self, body: str) -> FrozenDatum | None:
        """Decode one file-found body; log, count and return ``None`` if unusable.

        The catch-all is needed: a body of ``[]``, ``null`` or a bare number
        raises ``AttributeError`` in ``FrozenDatum.from_string``, and on a
        durable queue a body that kills the consumer is redelivered, so one
        malformed message could wedge every replica in turn.
        """
        try:
            return FrozenDatum.from_string(body)
        except Exception:  # parser boundary: anything raised here is poison
            JOB_BUILDER_MALFORMED_MESSAGES.labels(**self._metric_labels).inc()
            self._logger.exception(
                "Dropping malformed file-found message for builder %s "
                "(%d bytes); first %d shown: %r",
                self.identifier,
                len(body),
                _MALFORMED_BODY_PREVIEW,
                body[:_MALFORMED_BODY_PREVIEW],
            )
            return None

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

    def _process_job_group(self, job_group: JobGroup, file: FrozenDatum) -> None:
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
                    self._emit_ready_job(job_group, ready_job, targets)

            self._cleanup_old_jobs(job_group)

    def _emit_ready_job(
        self,
        job_group: JobGroup,
        job: Job,
        targets: tuple[str, ...],
    ) -> None:
        """Emit one job completed by the file path; trace and count the outcome.

        Only a published job is traced as emitted; a dropped or unclaimed one
        is not counted as built, and one whose every publish failed still is.
        """
        outcome = self._emit_job(job, targets)
        span = get_current_span()
        attributes = {
            ATTR_JOB_ID: job.identifier,
            ATTR_CORRELATION_ID: job.correlation_id,
        }
        span.add_event("job.ready", attributes=attributes)
        if outcome is _EmitOutcome.UNCLAIMED:
            self._return_files_to_group(job_group, job)
            return
        if outcome is _EmitOutcome.DROPPED:
            return
        if outcome is _EmitOutcome.PUBLISHED:
            span.add_event("job.emitted", attributes=attributes)
        self._jobs_built.labels(status="ready", **self._metric_labels).inc()
        self._files_per_job.labels(**self._metric_labels).observe(len(job.files))

    @contextlib.contextmanager
    def _group_lock(self, job_group: JobGroup) -> Iterator[None]:
        """Hold *job_group*'s lock when state sync configured one, else no-op."""
        lock = self._group_locks.get(job_group.name)
        with lock if lock is not None else contextlib.nullcontext():
            yield

    def _add_file_locked(
        self,
        job_group: JobGroup,
        file: FrozenDatum,
    ) -> tuple[bool, list[Job], dict[str, Job]]:
        """Add a file under the group lock; collect ready jobs and sync updates.

        Returns
        -------
        tuple[bool, list[Job], dict[str, Job]]
            ``(added, ready_jobs, updates)`` where *updates* maps job IDs
            to the modified ``Job`` objects that should be pushed to Redis.
        """
        updates: dict[str, Job] = {}
        ready: list[Job] = []
        with self._group_lock(job_group):
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
        file: FrozenDatum,
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
        with self._group_lock(job_group):
            old_ids = [jid for jid, job in job_group.jobs.items() if job.is_old()]
            for job_id in old_ids:
                self._log_discard(job_id)
                del job_group.jobs[job_id]
        return old_ids

    def _log_discard(self, job_id: str) -> None:
        """Log and count a discarded job."""
        self._logger.info(f"Discarding old job {job_id}")
        self._jobs_discarded.labels(**self._metric_labels).inc()
        self._jobs_built.labels(status="old", **self._metric_labels).inc()

    def _push_deletions(self, group_name: str, deletions: list[str]) -> None:
        """Notify peers of deleted jobs (no-op when sync is disabled)."""
        if self._sync is None:
            return
        for job_id in deletions:
            self._sync.push_job_deletion(group_name, job_id)

    def _emit_ready_jobs(
        self,
        job_group: JobGroup,
        reason: str = "completed by a peer's files",
    ) -> list[Job]:
        """Emit any job in *job_group* that is now complete.

        Every caller removes ready jobs through this method: the file path, a
        peer's merge, hydration at startup, and both timeout reapers. It also
        pushes the deletions. A Redis field that outlives its job survives for
        ``job.timeout`` (24h by default) and is re-adopted as the bucket's
        open job on the next restart.

        Jobs are removed under the group lock before being published, and the
        shared claim decides which replica dispatches.

        Parameters
        ----------
        job_group : JobGroup
            Group to scan.
        reason : str, optional
            Why the scan is running, for the per-job log line.

        Returns
        -------
        list[Job]
            The jobs that reached a broker, which the timeout reapers count as
            timeout emissions. A dropped or failed job is absent, and so is a
            job every replica skipped, because its files went back into the
            group.
        """
        with self._group_lock(job_group):
            ready = self._claim_ready_jobs(job_group)
        if not ready:
            return []
        self._push_deletions(job_group.name, [job.identifier for job in ready])
        targets = self._targets_for_group(job_group)
        emitted: list[Job] = []
        for job in ready:
            self._logger.info(f"Job {job.identifier} {reason}; emitting")
            outcome = self._emit_job(job, targets)
            if outcome is _EmitOutcome.PUBLISHED:
                emitted.append(job)
            elif outcome is _EmitOutcome.UNCLAIMED:
                self._return_files_to_group(job_group, job)
        return emitted

    def _return_files_to_group(self, job_group: JobGroup, job: Job) -> None:
        """Put back the files of a job every target had already claimed.

        ``_record_job_emitted`` closed the bucket when the job was popped, so
        these files land in a fresh job with a new identifier and a new claim
        key. The claim that just rejected them is not retried, and the
        dispatcher's dedupe LRU sees no repeated identifier.

        Parameters
        ----------
        job_group : JobGroup
            Group the job was taken from.
        job : Job
            The job nothing accepted.
        """
        with self._group_lock(job_group):
            for file in job.files:
                job_group.add_file(file)

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
            # Keyed by the run-step identifier. Under the class name, two
            # builders of the same class in one config shared a keyspace and
            # could claim each other's emissions. Replicas of one run step
            # still share a keyspace, since they share an identifier.
            builder_name=self.identifier,
        )


#: Registry of job builder plugins, read from the ``courier.job_builders``
#: entry-point group. Hands back classes; ``PluginManager`` constructs them.
job_builders = ClassPluginRegistry(
    name="job_builders",
    group=f"{ENTRY_POINT_PREFIX}.job_builders",
    expected_base=JobBuilder,
    nested_values=[PAYLOAD_KEY],
)
