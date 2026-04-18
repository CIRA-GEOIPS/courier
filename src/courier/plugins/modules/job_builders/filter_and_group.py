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

import threading
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, model_validator

from courier.constants import PluginRunState
from courier.interfaces.module_based.job_builders import JobBuilder
from courier.metrics import JOB_BUILDER_TIMEOUT_EMISSIONS
from courier.types.job import Job, JobGroup

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File, FrozenFile

interface: str = "job_builders"
family: str = "standard"
name: str = "filter_and_group"


class FilterAndGroupConfig(BaseModel, frozen=True):
    """Validated configuration for the filter_and_group job builder.

    Attributes
    ----------
    files_per_job : int
        Fast-path emission threshold: once a job has this many files, it
        is emitted without waiting for the window to close.
    min_files : int
        Dropout-path minimum: on window timeout, only emit jobs that have
        at least this many files. Must be ``<= files_per_job``.
    window_timeout_seconds : float | None
        If set, jobs with at least ``min_files`` will be emitted after
        this many seconds have elapsed since the group's last file arrival.
        ``None`` disables the timeout path (legacy behavior).
    filters : dict[str, str]
        Metadata filter: a file is added to the job group only when every
        key/value pair matches the file's attribute of the same name.
    time_grouping : dict | None
        Optional ``timedelta`` kwargs (``weeks``/``hours``/``minutes``/
        ``seconds``) plus an optional ``start`` reference datetime used to
        bucket files into fixed-width time windows.
    """

    files_per_job: int = Field(default=5, ge=1)
    min_files: int = Field(default=1, ge=1)
    window_timeout_seconds: float | None = Field(default=None, gt=0)
    filters: dict[str, str] = Field(default_factory=dict)
    time_grouping: dict[str, Any] | None = None

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
    """Return ``True`` if every filter key/value matches the file's attribute."""
    return all(getattr(file, key, None) == value for key, value in filters.items())


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

        def add_file(self, file: File | FrozenFile) -> None:
            """Add *file* unless filters reject it or the job is already full."""
            if not _file_matches_filters(file, config.filters):
                return
            if len(self.files) >= config.files_per_job:
                return
            super().add_file(file)

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

    name: str = "filter_and_group"
    version: str = "2"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        self.validated_config = FilterAndGroupConfig.model_validate(config)
        self._logger.debug(
            "Initializing FilterAndGroupJobBuilder with config "
            f"{self.validated_config}",
        )
        self.job_groups = [FilterAndGroupJobGroup(self.validated_config)]
        # Always create per-group locks so the reaper thread can safely
        # share state with the main consumer (even when state_sync is off).
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
        for job in emitted:
            self._logger.info(
                f"Timeout reaper emitting job {job.identifier} "
                f"with {len(job.files)} files",
            )
            self.emit(job)
            JOB_BUILDER_TIMEOUT_EMISSIONS.labels(job_builder_name=self.name).inc()


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")
