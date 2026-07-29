"""Dummy job builder module for testing and demonstration purposes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, Field

from courier.interfaces.module_based.job_builders import JobBuilder
from courier.types.job import Job, JobGroup

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File, FrozenFile

# Module-level logger for DummyJob class (which doesn't inherit from ServicePlugin)
_module_logger = logging.getLogger(__name__)


class DummyJobBuilderConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`DummyJobBuilder`."""

    targets: list[str] | None = Field(
        default=None,
        description="Optional list of targets to route to",
    )


class DummyJob(Job):
    """A dummy job implementation for testing purposes.

    This job accepts up to one file and is always ready for processing.
    """

    def ready(self) -> bool:
        """Check if the job is ready for processing.

        Returns
        -------
        bool
            Always returns True, indicating the job is ready.
        """
        return True  # if len(self.files) > 0 else False

    def add_file(self, file: File | FrozenFile) -> bool:
        """Add a file to the job with a maximum limit of one file.

        Parameters
        ----------
        file : File
            The file to add to the job.

        Returns
        -------
        bool
            ``True`` if the file was added, ``False`` if the job rejected it.

        Notes
        -----
        If the job already contains one file, a warning is logged and the
        file is not added.
        """
        if len(self.files) >= 1:
            _module_logger.warning(
                "DummyJob: Maximum number of files reached; cannot add more.",
            )
            return False
        return super().add_file(file)


class DummyJobGroup(JobGroup):
    """A dummy job group that considers all files as relevant.

    Parameters
    ----------
    config : dict
        Configuration dictionary for the job group.

    Attributes
    ----------
    job : type
        The job class to use (DummyJob).
    """

    def __init__(self, config: dict) -> None:
        """Initialize the DummyJobGroup.

        Parameters
        ----------
        config : dict
            Configuration dictionary for the job group.
        """
        super().__init__("DummyJob", config)
        self.job = DummyJob

    def file_is_relevant(self, file: File | FrozenFile) -> bool:  # noqa: ARG002
        """Determine if a file is relevant to this job group.

        Parameters
        ----------
        file : File
            The file to check for relevance.

        Returns
        -------
        bool
            Always returns True, indicating all files are relevant.
        """
        return True

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        """Get the job ID associated with a file.

        Parameters
        ----------
        file : File
            The file to get the job ID from.

        Returns
        -------
        str
            The job ID associated with the file.
        """
        return super().get_job_ids_from_file(file)


class DummyJobBuilder(JobBuilder):
    """A dummy job builder for testing and demonstration purposes.

    This builder creates and manages DummyJob instances through a
    DummyJobGroup, accepting all incoming files.

    Attributes
    ----------
    name : str
        The name of the job builder.
    version : str
        The version of the job builder.
    config : dict
        Configuration dictionary for the builder.
    job_groups : list of DummyJobGroup
        List containing the dummy job group.
    """

    interface: ClassVar[str] = "job_builders"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "DummyJobBuilder"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        """Initialize the DummyJobBuilder.

        Parameters
        ----------
        service : Service
            The service instance for the builder.
        config : dict
            Configuration dictionary for the builder.
        identifier : str or None
            Run-step identifier from the service YAML (optional).
        """
        super().__init__(service, config, identifier=identifier)
        cfg = config or {}
        self.validated = DummyJobBuilderConfig.model_validate(cfg)
        self.config = cfg
        self.job_groups = [DummyJobGroup(self.config)]

    def is_healthy(self) -> bool:
        """Return the health status of the builder.

        Returns
        -------
        bool
            Always returns True, indicating the builder is healthy.
        """
        return True

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate.

        Processes all incoming files and delegates to the parent class
        implementation.

        Returns
        -------
        None
        """
        self._logger.debug("DummyJobBuilder handling incoming files")
        return super().handle_incoming_files()

