"""Job and JobGroup domain types."""

import json
import time
from typing import Any, Never

from lazylemon.types.file import File, FrozenFile


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
    files : set[File | FrozenFile] | frozenset[Never], optional
        Files associated with this job.
    last_modified : float or None, optional
        Unix timestamp of last modification. Defaults to current time.
    timeout : float, optional
        Seconds before job is considered old. Defaults to 24 hours.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        identifier: str,
        config: Any,
        files: set[File | FrozenFile] | frozenset[Never] = frozenset(),
        last_modified: float | None = None,
        timeout: float = 60 * 60 * 24,
    ) -> None:
        self.name = name
        self.identifier = identifier
        self.config = config
        self.files = files
        self.last_modified = last_modified if last_modified is not None else time.time()
        self.timeout = timeout
        if self.files == frozenset():
            self.files = set()

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
            },
        )

    @classmethod
    def from_string(cls, s: str) -> "Job":
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
        )

    def ready(self) -> bool:
        """Return true if job is ready to be emitted."""
        return False

    def add_file(self, file: File | FrozenFile) -> None:
        """Add file to job."""
        self.files.add(file)  # type: ignore
        self.last_modified = time.time()

    def is_old(self) -> bool:
        """Return true if job is old and ready to be discarded."""
        return time.time() - self.last_modified > self.timeout


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

    def file_is_relevant(self, file: File | FrozenFile) -> bool:  # noqa: ARG002
        """Return true if file is relevant to this job group."""
        return False

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Return job ID from file."""
        return [str(file.file)]

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add file to appropriate job in job group.

        Return true if file was added to a job, false otherwise.
        """
        if not self.file_is_relevant(file):
            return False
        job_ids = self.get_job_ids_from_file(file)
        for job_id in job_ids:
            if job_id in self.jobs:
                self.jobs[job_id].add_file(file)
            else:
                self.jobs[job_id] = self.job(self.name, job_id, self.config)
                self.jobs[job_id].add_file(file)
        return True
