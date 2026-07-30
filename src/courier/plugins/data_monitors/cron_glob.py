"""Cron-scheduled glob-based Data Monitor Plugin for Courier.

This plugin fills the gap between Courier's event-driven monitors
(watchdog, RabbitMQ) and batch workflows that just need to scan a
directory on a fixed schedule.

Design decisions
----------------
**Scheduler — ``croniter``**
    Chosen over ``apscheduler`` because APScheduler brings its own event
    loop and thread pool, which conflicts with the generator-based
    ``find_file()`` loop where the plugin controls its own sleep.
    Fixed-interval sleep was also rejected: cron expressions are more
    expressive (e.g. "weekdays at 08:00") and a well-understood standard.

**File matching — ``pathlib.Path.glob()``**
    Standard library; handles both flat and recursive (``**``) patterns
    with no extra dependencies.

**Deduplication — bounded in-memory ``OrderedDict``**
    File paths are tracked in an ``OrderedDict`` capped at
    ``max_seen_files`` (default 100 000). The oldest entry is evicted
    when the cap is exceeded. The seen-set is only accessed from the
    single plugin thread, so no lock is needed.

    Persistent deduplication (SQLite, file-based) was rejected as
    unnecessary: downstream job builders are already idempotent with
    respect to duplicate file emissions, and restart re-scan is the
    safe default.

Tradeoffs
---------
- **Memory**: ~100 bytes per entry → default cap uses ~10 MB.
- **Restart**: the seen-set is lost on restart; all matching files are
  re-emitted unless ``ignore_existing=True`` pre-seeds the set.
- **No mtime tracking**: a file with the same path but new content is
  not re-emitted. A ``track_mtime`` option could be added later.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, field_validator

from courier.errors import InvalidPluginConfigError
from courier.interfaces.data_monitors import DataMonitorBasePlugin
from courier.metrics import (
    DATA_MONITOR_LAST_SCAN_TIMESTAMP,
    DATA_MONITOR_SCAN_DURATION,
)
from courier.types.file import File

if TYPE_CHECKING:
    from collections.abc import Generator

    from courier.service import Service


def _croniter() -> Any:
    """Return the ``croniter`` class, or explain which extra supplies it.

    Imported lazily rather than at module scope so courier installs without
    ``croniter`` unless this monitor is actually used. Entry-point discovery
    imports a plugin module only when a config names it, so an operator who
    never runs ``cron_glob`` never needs the dependency.
    """
    try:
        from croniter import croniter  # noqa: PLC0415
    except ImportError as exc:
        raise InvalidPluginConfigError(
            "cron_glob requires the cron extra: pip install courier[cron]",
        ) from exc
    return croniter


class CronGlobConfig(BaseModel, frozen=True):
    """Configuration for the cron_glob data monitor plugin.

    Attributes
    ----------
    path : str
        Directory to scan for files.
    glob_pattern : str
        Glob pattern for matching files (supports ``**`` for recursive).
    cron_expression : str
        Standard 5-field cron expression (e.g. ``*/5 * * * *``).
    max_seen_files : int
        Maximum number of file paths to remember for deduplication.
    hostname : str
        Hostname to set on emitted ``File`` objects.
    run_on_start : bool
        Whether to scan immediately on startup before waiting for the
        first cron tick.
    ignore_existing : bool
        If ``True``, pre-seed the seen-set with all currently matching
        files at startup so only newly arrived files are emitted.
    """

    path: str
    glob_pattern: str = "*"
    cron_expression: str = "0 * * * *"
    max_seen_files: int = 100_000
    hostname: str = "localhost"
    run_on_start: bool = True
    ignore_existing: bool = False

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        """Validate that the cron expression is a valid 5-field cron string."""
        if not _croniter().is_valid(v):
            msg = f"Invalid cron expression: '{v}'"
            raise ValueError(msg)
        return v


class CronGlob(DataMonitorBasePlugin):
    """Cron-scheduled glob-based data monitor.

    Periodically scans a directory using a glob pattern on a cron
    schedule and emits any new (previously unseen) files into the
    pipeline.
    """

    interface: ClassVar[str] = "data_monitors"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "cron_glob"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service,
        config: dict[str, Any] | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        validated = CronGlobConfig.model_validate(config or {})
        # expanduser() so "~/data" behaves the way operators (and the
        # quick-start guide) expect rather than looking for a literal "~" dir.
        self.scan_path = Path(validated.path).expanduser()
        self.glob_pattern = validated.glob_pattern
        self.cron_expression = validated.cron_expression
        self.max_seen_files = validated.max_seen_files
        self.hostname = validated.hostname
        self.run_on_start = validated.run_on_start
        self.ignore_existing = validated.ignore_existing
        self.health = False
        self._stop_event = threading.Event()
        self._seen: OrderedDict[Path, None] = OrderedDict()

    def stop(self) -> None:
        """Signal the plugin to stop and join the main thread."""
        self._stop_event.set()
        super().stop()

    def is_healthy(self) -> bool:
        """Check if the data monitor is healthy."""
        return self.health

    def find_file(self) -> Generator[File, None, None]:
        """Scan a directory on a cron schedule and yield new files.

        Yields
        ------
        File
            A ``File`` object for each newly discovered file.
        """
        if not self.scan_path.is_dir():
            msg = f"Directory '{self.scan_path}' does not exist."
            raise RuntimeError(msg)

        cron = _croniter()(self.cron_expression, datetime.now())

        try:
            self.health = True

            if self.ignore_existing:
                self._seed_seen()

            if self.run_on_start:
                yield from self._scan_directory()

            while not self._stop_event.is_set():
                next_time: datetime = cron.get_next(datetime)
                if self._wait_until(next_time):
                    return
                yield from self._scan_directory()
        finally:
            self.health = False

    def _seed_seen(self) -> None:
        """Pre-populate the seen-set with all currently matching files.

        Respects ``max_seen_files``: if there are more pre-existing files
        than the cap, the oldest entries are evicted so the set stays bounded.
        """
        count = 0
        for path in self.scan_path.glob(self.glob_pattern):
            if path.is_file():
                self._seen[path.resolve()] = None
                self._evict_if_over_cap()
                count += 1
        self._logger.debug(f"Pre-seeded {count} existing files into seen-set")

    def _scan_directory(self) -> Generator[File, None, None]:
        """Glob the directory and yield unseen files.

        Each newly discovered file is added to the seen-set immediately
        and the cap is enforced inline, so the set never exceeds
        ``max_seen_files`` even if the caller stops consuming mid-scan.

        Yields
        ------
        File
            A ``File`` object for each new file found.
        """
        scan_start = time.time()
        for path in self.scan_path.glob(self.glob_pattern):
            if not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in self._seen:
                continue
            self._seen[resolved] = None
            self._evict_if_over_cap()
            yield File(file=resolved, hostname=self.hostname)

        DATA_MONITOR_LAST_SCAN_TIMESTAMP.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).set(time.time())
        DATA_MONITOR_SCAN_DURATION.labels(
            monitor_name=self.name,
            monitor_identifier=self.identifier,
        ).observe(time.time() - scan_start)

    def _evict_if_over_cap(self) -> None:
        """Remove the oldest entry from the seen-set if the cap is exceeded."""
        if len(self._seen) > self.max_seen_files:
            self._seen.popitem(last=False)

    def _wait_until(self, target: datetime) -> bool:
        """Sleep until *target* time, checking stop event each second.

        Parameters
        ----------
        target : datetime
            The time to wait until.

        Returns
        -------
        bool
            ``True`` if the stop event was set (caller should exit),
            ``False`` if the target time was reached normally.
        """
        while datetime.now() < target:
            if self._stop_event.wait(timeout=1.0):
                return True
        return False

