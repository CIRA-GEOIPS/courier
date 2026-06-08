"""Filter-and-group job builder with optional timeout-based emission.

Emits jobs when either:

* The number of accumulated files reaches ``files_per_job`` (fast path), or
* The time window since the first file exceeds ``window_timeout_seconds``
  AND at least ``min_files`` have accumulated (dropout path).

The dropout path protects against upstream gaps: if a satellite feed
drops a file, the job builder still emits a partial job instead of
waiting indefinitely for a file that never arrives.
"""

from __future__ import annotations

import logging
import threading
import time
import types
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from courier.constants import PluginRunState
from courier.interfaces.module_based.job_builders import JobBuilder
from courier.metrics import JOB_BUILDER_TIMEOUT_EMISSIONS
from courier.types.job import Job, JobGroup

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File, FrozenFile

_module_logger = logging.getLogger(__name__)


class FilterAndGroupConfig(BaseModel, frozen=True):
    """Validated configuration for the filter_and_group job builder."""

    files_per_job: int = Field(default=5, ge=1)
    min_files: int = Field(default=1, ge=1)
    window_timeout_seconds: float | None = Field(default=None, gt=0)
    filters: dict[str, str] = Field(default_factory=dict)
    time_grouping: dict[str, Any] | None = None
    targets: list[str] | None = Field(
        default=None,
        description=(
            "Dispatcher identifiers this builder's jobs should be "
            "published to. ``None`` is resolved at preflight via the "
            "service's ``allow_implicit_target`` policy."
        ),
    )

    # ------------------------------------------------------------------ #
    # Serialization                                                        #
    # ------------------------------------------------------------------ #

    @field_serializer("time_grouping")
    def _serialize_time_grouping(
        self,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Convert any ``datetime`` values to ISO-8601 strings."""
        if value is None:
            return None
        return {
            k: v.isoformat() if isinstance(v, datetime) else v for k, v in value.items()
        }

    # ------------------------------------------------------------------ #
    # Deserialization                                                      #
    # ------------------------------------------------------------------ #

    @field_validator("time_grouping", mode="before")
    @classmethod
    def _parse_time_grouping(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Parse ISO-8601 strings back to ``datetime`` under ``"start"``."""
        if value is None:
            return None
        if "start" in value and isinstance(value["start"], str):
            value = {**value, "start": datetime.fromisoformat(value["start"])}
        return value

    # ------------------------------------------------------------------ #
    # Cross-field validation                                               #
    # ------------------------------------------------------------------ #

    @model_validator(mode="after")
    def _check_min_files(self) -> FilterAndGroupConfig:
        if self.min_files > self.files_per_job:
            msg = (
                f"min_files ({self.min_files}) must be <= "
                f"files_per_job ({self.files_per_job})"
            )
            raise ValueError(msg)
        return self


def _file_matches_filters(
    file: File | FrozenFile,
    filters: dict[str, str],
) -> bool:
    """Return ``True`` if every filter key/value matches the file.

    Checks, in order, for each filter key:
    1. ``file.metadata.get(key)`` — metadata dict keys (from field_map extras)
    2. ``getattr(file, key, None)`` — File dataclass attributes

    If the key is found in neither layer, logs a WARNING and returns ``False``.
    """
    for key, value in filters.items():
        # Layer 1: check metadata dict
        if key in file.metadata:
            if file.metadata[key] != value:
                return False
            continue
        # Layer 2: check File dataclass attribute
        attr_value = getattr(file, key, None)
        if attr_value is not None:
            if attr_value != value:
                return False
            continue
        # Key not found in either layer
        _module_logger.warning(
            "Unknown filter key %r: not found in file metadata or "
            "File attributes (source, instrument, processing_stage, domain, "
            "hostname, num_expected, timestamp). "
            "Metadata keys: %s",
            key,
            list(file.metadata.keys()),
        )
        return False
    return True


def make_job_class(config: FilterAndGroupConfig) -> type[Job]:
    """Build a Job subclass whose ``ready()`` method applies *config*.

    A fresh class is produced per ``JobGroup`` instance so that the
    configured thresholds are captured in the closure without having to
    pass them through every ``add_file`` / ``ready`` call.
    """

    class FilterAndGroupJob(Job):
        """Job that emits when file-count or window-timeout threshold is met."""

        def ready(self) -> bool:
            """Check file-count fast path and window-timeout dropout path."""
            if len(self.files) >= config.files_per_job:
                return True
            if config.window_timeout_seconds is None:
                return False
            elapsed = time.time() - self.last_modified
            return (
                elapsed >= config.window_timeout_seconds
                and len(self.files) >= config.min_files
            )

        def add_file(self, file: File | FrozenFile) -> bool:
            """Add *file* unless filters reject it or the job is already full.

            Returns
            -------
            bool
                ``True`` if the file was added, ``False`` if rejected.
            """
            if not _file_matches_filters(file, config.filters):
                return False
            if len(self.files) >= config.files_per_job:
                return False
            return super().add_file(file)

    return FilterAndGroupJob


class FilterAndGroupJobGroup(JobGroup):
    """Group files by optional time window and metadata filters."""

    def __init__(
        self,
        config: dict | FilterAndGroupConfig,
        group_name: str = "FilterAndGroupJob",
    ) -> None:
        validated = (
            config
            if isinstance(config, FilterAndGroupConfig)
            else FilterAndGroupConfig.model_validate(config)
        )
        super().__init__(group_name, validated)
        self.validated_config = validated
        self.filters = validated.filters
        self.number_of_files = validated.files_per_job
        self.time_grouping = validated.time_grouping
        self.job = make_job_class(validated)

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        """Return ``True`` when file metadata satisfies the configured filters."""
        return _file_matches_filters(file, self.filters)

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Bucket the file's timestamp into a time-group ID string."""
        if self.time_grouping is None:
            return super().get_job_ids_from_file(file)
        if file.timestamp is None:
            return []
        delta = timedelta(
            weeks=float(self.time_grouping.get("weeks", 0)),
            hours=float(self.time_grouping.get("hours", 0)),
            minutes=float(self.time_grouping.get("minutes", 0)),
            seconds=float(self.time_grouping.get("seconds", 0)),
        )
        if delta.total_seconds() <= 0:
            return []
        start_raw = self.time_grouping.get("start", "1900-01-01 00:00:00")
        start_datetime = (
            start_raw
            if isinstance(start_raw, datetime)
            else datetime.strptime(str(start_raw), "%Y-%m-%d %H:%M:%S")
        )
        bucket = int(
            (file.timestamp.timestamp() - start_datetime.timestamp())
            // delta.total_seconds(),
        )
        return [str(bucket)]


class FilterAndGroupJobBuilder(JobBuilder):
    """Job builder with metadata filtering, time grouping, and timeout emission.

    Thread-safe: ``_group_locks[group.name]`` protects each group's ``jobs``
    dict. The reaper thread and the main file-consuming thread both acquire
    the lock before mutating or reading the group.
    """

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "filter_and_group"
    version: ClassVar[str] = "2"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated_config = FilterAndGroupConfig.model_validate(config or {})
        self._logger.debug(
            "Initializing FilterAndGroupJobBuilder with config "
            f"{self.validated_config}",
        )
        self.job_groups = [FilterAndGroupJobGroup(self.validated_config)]
        self._group_locks = {jg.name: threading.Lock() for jg in self.job_groups}
        self._reaper_stop_event = threading.Event()
        self._reaper_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the main consumer and, when configured, the reaper thread."""
        super().start()
        if self.validated_config.window_timeout_seconds is not None:
            self._reaper_stop_event.clear()
            self._reaper_thread = threading.Thread(
                target=self._run_reaper,
                name=f"{self.name}-reaper",
                daemon=True,
            )
            self._reaper_thread.start()

    def stop(self) -> None:
        """Stop the reaper thread then the main consumer."""
        self._reaper_stop_event.set()
        if self._reaper_thread and self._reaper_thread.is_alive():
            self._reaper_thread.join(timeout=5)
        super().stop()

    def is_healthy(self) -> bool:
        """Healthy only while running and the reaper (if any) is still alive."""
        if self._state != PluginRunState.RUNNING:
            return False
        return self._reaper_thread is None or self._reaper_thread.is_alive()

    def _reaper_interval(self) -> float:
        timeout = self.validated_config.window_timeout_seconds or 60.0
        return min(max(timeout / 2, 1.0), 30.0)

    def _run_reaper(self) -> None:
        """Emit jobs whose window has expired. Thread-safe via group locks."""
        interval = self._reaper_interval()
        self._logger.debug(
            f"Reaper thread started with interval={interval:.1f}s "
            f"window_timeout={self.validated_config.window_timeout_seconds:.1f}s",
        )
        while not self._reaper_stop_event.wait(timeout=interval):
            for job_group in self.job_groups:
                self._reap_group(job_group)

    def _reap_group(self, job_group: JobGroup) -> None:
        """Emit ready jobs from *job_group* and delete them under its lock."""
        lock = self._group_locks.get(job_group.name)
        if lock is None:
            return
        with lock:
            ready_ids = [jid for jid, job in job_group.jobs.items() if job.ready()]
            emitted: list[Job] = []
            for jid in ready_ids:
                emitted.append(job_group.jobs.pop(jid))
                job_group._record_job_emitted(jid)
        for job in emitted:
            self._logger.info(
                f"Timeout reaper emitting job {job.identifier} "
                f"with {len(job.files)} files",
            )
            self.emit(job, self.targets)
            JOB_BUILDER_TIMEOUT_EMISSIONS.labels(job_builder_name=self.name).inc()


PLUGIN_CLASS = FilterAndGroupJobBuilder
