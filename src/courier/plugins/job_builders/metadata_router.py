"""Route incoming files to different job groups by metadata filters.

One plugin instance can service multiple sensor types / processing pipelines.
Each route owns an independent :class:`FilterAndGroupJobGroup`. Routes are
evaluated in declaration order and the first match wins — files that match
no route are counted as unmatched and dropped.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, Field, field_validator, model_validator

from courier.constants import PluginRunState
from courier.interfaces.job_builders import JobBuilder
from courier.metrics import (
    JOB_BUILDER_ROUTE_MATCHES,
    JOB_BUILDER_TIMEOUT_EMISSIONS,
    JOB_BUILDER_UNMATCHED_FILES,
)
from courier.plugins.job_builders.filter_and_group import (
    FilterAndGroupConfig,
    FilterAndGroupJobGroup,
)
from courier.tracing import ATTR_FILE_PATH, ATTR_FILE_SOURCE, get_tracer
from courier.types.file import FrozenFile

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job, JobGroup


class RouteConfig(BaseModel, frozen=True):
    """Single route within :class:`MetadataRouterConfig`.

    Attributes
    ----------
    name : str
        Unique route label (used in metrics and logs).
    filters : dict[str, str]
        Equality filters checked against File dataclass attributes
        (e.g. ``source``, ``instrument``).
    metadata_filters : dict[str, str]
        Equality filters checked against ``file.metadata`` keys
        (e.g. ``level``, ``domain``).  Merged with *filters* before
        the file-is-relevant check so a route can require both
        ``source: goes-18`` AND ``level: cwc``.
    files_per_job : int
        Fast-path emission threshold for this route.
    min_files : int
        Dropout-path minimum for this route.
    window_timeout_seconds : float | None
        Timeout-based emission window for this route.
    time_grouping : dict | None
        Optional time bucketing config; same semantics as
        :class:`FilterAndGroupConfig`.
    """

    name: str
    filters: dict[str, str] = Field(default_factory=dict)
    metadata_filters: dict[str, str] = Field(default_factory=dict)
    files_per_job: int = Field(default=1, ge=1)
    min_files: int = Field(default=1, ge=1)
    window_timeout_seconds: float | None = Field(default=None, gt=0)
    time_grouping: dict[str, float | int | str] | None = None
    targets: list[str] | None = Field(
        default=None,
        description=(
            "Dispatcher identifiers this route's jobs should be published "
            "to. ``None`` is resolved at preflight via the service's "
            "``allow_implicit_target`` policy."
        ),
    )

    @model_validator(mode="after")
    def _check_min_files(self) -> RouteConfig:
        if self.min_files > self.files_per_job:
            msg = (
                f"min_files ({self.min_files}) must be <= "
                f"files_per_job ({self.files_per_job}) in route {self.name!r}"
            )
            raise ValueError(msg)
        return self

    def to_filter_and_group_config(self) -> FilterAndGroupConfig:
        """Project this route onto a :class:`FilterAndGroupConfig`.

        Merges ``filters`` and ``metadata_filters`` into a single filter
        dict so that ``_file_matches_filters`` checks both File dataclass
        attributes (e.g. ``source``) and metadata-dict keys (e.g. ``level``).
        """
        merged_filters = {**self.filters, **self.metadata_filters}
        return FilterAndGroupConfig(
            files_per_job=self.files_per_job,
            min_files=self.min_files,
            window_timeout_seconds=self.window_timeout_seconds,
            filters=merged_filters,
            time_grouping=self.time_grouping,
        )


class MetadataRouterConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`MetadataRouterBuilder`."""

    routes: list[RouteConfig] = Field(min_length=1)

    @field_validator("routes")
    @classmethod
    def _unique_route_names(cls, v: list[RouteConfig]) -> list[RouteConfig]:
        names = [route.name for route in v]
        if len(set(names)) != len(names):
            raise ValueError(f"Route names must be unique, got: {names}")
        return v


RouteConfig.model_rebuild()
MetadataRouterConfig.model_rebuild()


class MetadataRouterBuilder(JobBuilder):
    """Job builder that routes files to per-route :class:`FilterAndGroupJobGroup`.

    Thread-safe: ``_group_locks[group.name]`` protects each route's
    ``jobs`` dict. The reaper thread (if any route has a timeout) and the
    main consumer both acquire the lock before mutating state.
    """

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "metadata_router"
    version: ClassVar[str] = "1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        self.validated_config = MetadataRouterConfig.model_validate(config or {})

        self.job_groups = [
            FilterAndGroupJobGroup(
                route.to_filter_and_group_config(),
                group_name=f"metadata_router:{route.name}",
            )
            for route in self.validated_config.routes
        ]
        self._route_names = [route.name for route in self.validated_config.routes]
        self._group_locks = {jg.name: threading.Lock() for jg in self.job_groups}
        self._route_targets: dict[str, tuple[str, ...]] = {
            f"metadata_router:{route.name}": tuple(route.targets or ())
            for route in self.validated_config.routes
        }

        self._reaper_stop_event = threading.Event()
        self._reaper_thread: threading.Thread | None = None
        self._has_timeout = any(
            route.window_timeout_seconds is not None
            for route in self.validated_config.routes
        )

    def start(self) -> None:
        """Start main consumer; launch reaper if any route has a timeout."""
        super().start()
        if self._has_timeout:
            self._reaper_stop_event.clear()
            self._reaper_thread = threading.Thread(
                target=self._run_reaper,
                name=f"{self.name}-reaper",
                daemon=True,
            )
            self._reaper_thread.start()

    def stop(self) -> None:
        """Stop reaper then main consumer."""
        self._reaper_stop_event.set()
        if self._reaper_thread and self._reaper_thread.is_alive():
            self._reaper_thread.join(timeout=5)
        super().stop()

    def is_healthy(self) -> bool:
        """Healthy only while running and the reaper (if any) is alive."""
        if self._state != PluginRunState.RUNNING:
            return False
        return self._reaper_thread is None or self._reaper_thread.is_alive()

    def handle_incoming_files(self) -> None:
        """Loop over files, applying first-match routing and unmatched metrics."""
        import time  # noqa: PLC0415

        from courier.constants import FILE_FOUND_EXCHANGE  # noqa: PLC0415

        tracer = get_tracer(__name__)
        self._logger.debug("metadata_router starting file consumption")
        for file_string, parent_ctx in self.parent_service.consume(
            FILE_FOUND_EXCHANGE,
            stop_event=self._stop_event,
            on_subscribed=self._subscribed.set,
        ):
            start_time = time.time()
            with tracer.start_as_current_span(
                "metadata_router.route_file",
                context=parent_ctx,
            ) as span:
                self._files_received.labels(
                    job_builder_name=self.name,
                    job_builder_identifier=self.identifier,
                ).inc()
                file = FrozenFile.from_string(str(file_string))
                span.set_attribute(ATTR_FILE_PATH, str(file.file) if file.file else "")
                span.set_attribute(ATTR_FILE_SOURCE, file.source or "")
                matched = False
                for jg, route_name in zip(
                    self.job_groups,
                    self._route_names,
                    strict=True,
                ):
                    if not jg.file_is_relevant(file):
                        continue
                    self._process_job_group(jg, file)
                    JOB_BUILDER_ROUTE_MATCHES.labels(
                        job_builder_name=self.name,
                        job_builder_identifier=self.identifier,
                        route_name=route_name,
                    ).inc()
                    matched = True
                    break
                if not matched:
                    self._logger.debug(f"No route matched file {file}")
                    JOB_BUILDER_UNMATCHED_FILES.labels(
                        job_builder_name=self.name,
                        job_builder_identifier=self.identifier,
                    ).inc()
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

    def _targets_for_group(self, job_group: JobGroup) -> tuple[str, ...]:
        """Return per-route targets, falling back to the builder default.

        A route that declares no ``targets`` is stored as an empty tuple, not
        as a missing key, so ``dict.get(name, default)`` never reaches its
        default. Fall back on the *value* being empty instead -- otherwise
        preflight's ``allow_implicit_target`` auto-wiring is discarded and
        :meth:`JobBuilder.emit` drops every job from that route.
        """
        return self._route_targets.get(job_group.name) or self.targets

    def _reaper_interval(self) -> float:
        timeouts = [
            r.window_timeout_seconds
            for r in self.validated_config.routes
            if r.window_timeout_seconds is not None
        ]
        if not timeouts:
            return 30.0
        return min(max(min(timeouts) / 2, 1.0), 30.0)

    def _run_reaper(self) -> None:
        """Emit jobs whose window has expired across all routes."""
        interval = self._reaper_interval()
        self._logger.debug(f"metadata_router reaper started (interval={interval:.1f}s)")
        while not self._reaper_stop_event.wait(timeout=interval):
            for job_group in self.job_groups:
                self._reap_group(job_group)

    def _reap_group(self, job_group: JobGroup) -> None:
        """Emit and delete ready jobs in *job_group* under its lock."""
        lock = self._group_locks.get(job_group.name)
        if lock is None:
            return
        with lock:
            ready_ids = [jid for jid, job in job_group.jobs.items() if job.ready()]
            emitted: list[Job] = []
            for jid in ready_ids:
                emitted.append(job_group.jobs.pop(jid))
                # Must bump the overflow counter on every removal, exactly as
                # FilterAndGroupJobBuilder._reap_group does. Skipping it lets a
                # later file re-derive an ID that is still live in self.jobs,
                # which JobGroup.add_file would then overwrite.
                job_group._record_job_emitted(jid)
        targets = self._targets_for_group(job_group)
        for job in emitted:
            self._logger.info(
                f"Timeout reaper emitting route job {job.identifier} "
                f"with {len(job.files)} files",
            )
            self.emit(job, targets)
            JOB_BUILDER_TIMEOUT_EMISSIONS.labels(
                job_builder_name=self.name,
                job_builder_identifier=self.identifier,
            ).inc()
