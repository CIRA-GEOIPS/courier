"""Job and JobGroup domain types."""

from __future__ import annotations

import json
import time
import uuid
from typing import TYPE_CHECKING, Any

import pydantic

if TYPE_CHECKING:
    from collections.abc import Iterable

from courier.types.file import File, FrozenFile

_OVERFLOW_SEPARATOR = "_overflow_"


# Mutable because: Job accumulates files incrementally via add_file() until
# ready() returns True; single-threaded ownership by the job builder plugin.
class Job:
    """Job class representing a processing task.

    Parameters
    ----------
    name : str
        Name of the job.
    identifier : str
        Unique identifier for this job instance.
    config : Any
        Job configuration.
    files : Iterable[File | FrozenFile] or None, optional
        Files associated with this job. None or empty means no initial files.
    last_modified : float or None, optional
        Unix timestamp of last modification. Defaults to current time.
    timeout : float, optional
        Seconds before job is considered old. Defaults to 24 hours.
    correlation_id : str or None, optional
        UUID propagated from the originating file; generated on first
        construction when absent so every Job has a stable ID for log
        correlation across the data-monitor → builder → dispatcher
        pipeline.
    emit_time : float or None, optional
        Unix timestamp stamped by the job builder at emit. Used by the
        dispatcher to compute end-to-end routing latency. ``None`` until
        the builder has published the job.
        targets : tuple[str, ...] or None, optional
            Observability record of which dispatcher identifiers the builder
            published this job to. Not used for routing — routing is by
            queue name — but preserved round-trip for debugging and
            provenance. Stored as a tuple so the field remains effectively
            immutable despite the surrounding class being mutable.

    Notes
    -----
        ``config`` may be any type, but serialization via :meth:`__str__`
        converts Pydantic models to plain dicts via ``model_dump()``.
        Deserialization via :meth:`from_string` reconstructs ``config`` as
        a dict.  Consumers should be prepared for either form.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        identifier: str,
        config: Any,
        files: Iterable[File | FrozenFile] | None = None,
        last_modified: float | None = None,
        timeout: float = 60 * 60 * 24,
        correlation_id: str | None = None,
        emit_time: float | None = None,
        targets: tuple[str, ...] | None = None,
    ) -> None:
        self.name = name
        self.identifier = identifier
        self.config = config
        self.files: set[FrozenFile] = (
            {f.freeze() if isinstance(f, File) else f for f in files}
            if files is not None
            else set()
        )
        self.last_modified = last_modified if last_modified is not None else time.time()
        self.timeout = timeout
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.emit_time = emit_time
        self.targets: tuple[str, ...] = tuple(targets) if targets else ()

    def __str__(self) -> str:
        """Convert Job to JSON string."""

        def _json_default(obj: Any) -> Any:
            if isinstance(obj, pydantic.BaseModel):
                return obj.model_dump()
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable",
            )

        return json.dumps(
            {
                "name": self.name,
                "identifier": self.identifier,
                "config": self.config,
                "files": [str(f) for f in self.files],
                "last_modified": self.last_modified,
                "timeout": self.timeout,
                "correlation_id": self.correlation_id,
                "emit_time": self.emit_time,
                "targets": list(self.targets),
            },
            default=_json_default,
        )

    @classmethod
    def from_string(cls, s: str) -> Job:
        """Initialize Job from JSON string.

        Parameters
        ----------
        s : str
            JSON string representation of Job.

        Returns
        -------
        Job
            Job instance.
        """
        data = json.loads(s)
        return cls(
            name=data["name"],
            identifier=data["identifier"],
            config=data["config"],
            files={FrozenFile.from_string(f) for f in data.get("files", [])},
            last_modified=data.get("last_modified"),
            timeout=data.get("timeout", 60 * 60 * 24),
            correlation_id=data.get("correlation_id"),
            emit_time=data.get("emit_time"),
            targets=tuple(data.get("targets") or ()),
        )

    def ready(self) -> bool:
        """Return true if job is ready to be emitted."""
        return False

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add file to job (freezes mutable File before adding).

        Returns
        -------
        bool
            ``True`` if the file was accepted, ``False`` if the job rejected it
            (e.g. capacity reached, filter mismatch).

        Notes
        -----
        The default implementation accepts every file unconditionally.
        Subclasses with capacity, filter, or other rejection criteria
        **must** override this method and return ``False`` on rejection
        so that :meth:`JobGroup.add_file` can create overflow jobs for
        rejected files.
        """
        if not isinstance(file, FrozenFile):
            file = file.freeze()
        self.files.add(file)
        self.last_modified = time.time()
        return True

    def is_old(self) -> bool:
        """Return true if job is old and ready to be discarded."""
        return time.time() - self.last_modified > self.timeout


# Mutable because: JobGroup holds a mutable dict of in-progress Jobs and
# routes incoming files to them; single-threaded ownership by the job builder.
class JobGroup:
    """Job group class for grouping related jobs.

    Parameters
    ----------
    job_name : str
        Name of jobs created by this group.
    config : Any
        Group configuration passed to created jobs.
    """

    def __init__(self, job_name: str, config: Any) -> None:
        self.name = job_name
        self.config = config
        self.jobs: dict[str, Job] = {}
        self.job = Job
        self._overflow_counters: dict[str, int] = {}

    def ready_jobs(self) -> list[Job]:
        """Return list of ready jobs."""
        return [self.jobs[jid] for jid in self.jobs if self.jobs[jid].ready()]

    def _record_job_emitted(self, job_id: str) -> None:
        """Bump the overflow counter for *job_id* so future jobs get unique IDs.

        Strips any ``_overflow_N`` suffix to extract the base bucket ID,
        then increments the counter.  Called by the job builder fast-path
        and the reaper timeout-path whenever a job is emitted and popped
        from the group.

        Notes
        -----
        ``_overflow_counters`` is never pruned — for very long-running
        services processing many unique base IDs, the dict grows
        unboundedly.  This is an accepted trade-off: each entry is
        ~100 bytes and the set of base IDs is bounded by the product of
        the time-window granularity and the service lifetime.
        """
        base_id = job_id
        if _OVERFLOW_SEPARATOR in base_id:
            base_id = base_id.rsplit(_OVERFLOW_SEPARATOR, 1)[0]
        self._overflow_counters[base_id] = self._overflow_counters.get(base_id, 0) + 1

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        """Return true if file is relevant to this job group."""
        raise NotImplementedError

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Return job ID from file."""
        return [str(file.file)]

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add file to appropriate job in job group.

        Return true if file was added to at least one job, false otherwise.

        When an existing job rejects the file (e.g. it is full), a new
        overflow job is created with a suffixed ID so the file is never
        silently dropped.
        """
        if not self.file_is_relevant(file):
            return False
        job_ids = self.get_job_ids_from_file(file)
        if not job_ids:
            return False
        for job_id in set(job_ids):
            if job_id in self.jobs:
                accepted = self.jobs[job_id].add_file(file)
                if not accepted:
                    # Existing job rejected the file — create overflow job.
                    self._overflow_counters[job_id] = (
                        self._overflow_counters.get(job_id, 0) + 1
                    )
                    overflow_id = (
                        f"{job_id}{_OVERFLOW_SEPARATOR}"
                        f"{self._overflow_counters[job_id]}"
                    )
                    overflow = self.job(
                        self.name,
                        overflow_id,
                        self.config,
                    )
                    if not overflow.add_file(file):
                        raise RuntimeError(
                            f"overflow job {overflow_id!r} rejected file {file!r}; "
                            f"Job.add_file may only return False for capacity/filter "
                            f"reasons; a fresh job should always accept the first file",
                        )
                    self.jobs[overflow_id] = overflow
            else:
                actual_id = job_id
                counter = self._overflow_counters.get(job_id, 0)
                if counter > 0:
                    actual_id = f"{job_id}{_OVERFLOW_SEPARATOR}{counter}"
                self.jobs[actual_id] = self.job(self.name, actual_id, self.config)
                self.jobs[actual_id].add_file(file)
        return True
