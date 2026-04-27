"""Job and JobGroup domain types."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterable
from typing import Any

from courier.types.file import File, FrozenFile


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
        self.files: set[File | FrozenFile] = set(files) if files is not None else set()
        self.last_modified = last_modified if last_modified is not None else time.time()
        self.timeout = timeout
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.emit_time = emit_time
        self.targets: tuple[str, ...] = tuple(targets) if targets else ()

    def __str__(self) -> str:
        """Convert Job to JSON string."""
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

    def add_file(self, file: File | FrozenFile) -> None:
        """Add file to job."""
        self.files.add(file)
        self.last_modified = time.time()

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

    def ready_jobs(self) -> list[Job]:
        """Return list of ready jobs."""
        return [self.jobs[jid] for jid in self.jobs if self.jobs[jid].ready()]

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        """Return true if file is relevant to this job group."""
        raise NotImplementedError

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Return job ID from file."""
        return [str(file.file)]

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add file to appropriate job in job group.

        Return true if file was added to at least one job, false otherwise.
        """
        if not self.file_is_relevant(file):
            return False
        job_ids = self.get_job_ids_from_file(file)
        if not job_ids:
            return False
        for job_id in set(job_ids):
            if job_id in self.jobs:
                self.jobs[job_id].add_file(file)
            else:
                self.jobs[job_id] = self.job(self.name, job_id, self.config)
                self.jobs[job_id].add_file(file)
        return True
