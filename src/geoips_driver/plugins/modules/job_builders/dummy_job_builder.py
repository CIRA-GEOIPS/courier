"""Dummy job builder module for testing and demonstration purposes."""

from geoips_driver.interfaces.module_based.job_builders import (
    Job,
    JobBuilder,
    JobGroup,
)
from geoips_driver.interfaces.module_based.service import Service, setup_logging
from geoips_driver.types.file import File

logger = setup_logging("dummy_job_builder")

interface: str = "job_builders"
family: str = "standard"
name: str = "dummy_job_builder"


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

    def add_file(self, file: File) -> None:
        """Add a file to the job with a maximum limit of one file.

        Parameters
        ----------
        file : File
            The file to add to the job.

        Returns
        -------
        None

        Notes
        -----
        If the job already contains one file, a warning is logged and the
        file is not added.
        """
        if len(self.files) >= 1:
            logger.warning(
                "DummyJob: Maximum number of files reached; cannot add more.",
            )
            return
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

    def file_is_relevant(self, file: File) -> bool:  # noqa: ARG002
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

    def get_job_id_from_file(self, file: File) -> str:
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
        return super().get_job_id_from_file(file)


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

    name: str = "DummyJobBuilder"
    version: str = "-1"

    def __init__(self, service: Service, config: dict) -> None:
        """Initialize the DummyJobBuilder.

        Parameters
        ----------
        service : Service
            The service instance for the builder.
        config : dict
            Configuration dictionary for the builder.
        """
        super().__init__(service, config)
        logger.debug(f"Initializing DummyJobBuilder with config {config}")
        self.config = config
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
        logger.debug("DummyJobBuilder handling incoming files")
        return super().handle_incoming_files()


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")
