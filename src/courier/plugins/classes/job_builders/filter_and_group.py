"""Job builder module that uses metadata filters, file count and groupings by time."""

from __future__ import annotations

import logging
import types
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

from courier.interfaces.module_based.job_builders import JobBuilder
from courier.types.job import Job, JobGroup

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import File, FrozenFile

_module_logger = logging.getLogger(__name__)


def create_job(
    number_of_files: int,
    time_grouping: dict[str, float | int] | None,  # noqa: ARG001
    filters: dict[str, str],
) -> type[Job]:
    """Create a job with specified number of files and time grouping.

    Parameters
    ----------
    number_of_files : int
        The number of files required for the job to be ready.
    time_grouping : dict[str, float|int] | None
        The time grouping dict following the format of python timedelta
        (e.g., {"hours": 1} for grouping by hour). If None, no time grouping is applied.

    Returns
    -------
    Job
        A new instance of a Job configured with the specified parameters.
    """

    class FilterAndGroupJob(Job):
        """A dummy job implementation for testing purposes.

        This job accepts files if they match the metadata filters.
        It groups them by time and counts the number of files in each group.

        Once the job is ready (i.e., there are 5 files in a time group), it is
        emitted as ready for processing.
        """

        def ready(self) -> bool:
            """Check if the job is ready for processing.

            Returns
            -------
            bool
                Always returns True, indicating the job is ready.
            """
            return len(self.files) >= number_of_files

        def add_file(self, file: File | FrozenFile) -> None:
            """Add a file to the job with a maximum limit of files.

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
            for key, value in filters.items():
                if file.__dict__[key] != value:
                    _module_logger.debug(
                        f"File {file} does not match filter {key}={value}; skipping.",
                    )
                    return
            if len(self.files) >= number_of_files:
                _module_logger.error(
                    "DummyJob: Maximum number of files reached; cannot add more.",
                )
                return
            return super().add_file(file)

    return FilterAndGroupJob


class FilterAndGroupJobGroup(JobGroup):
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
        self.filter = config.get("filters", {})
        self.number_of_files = int(config.get("files_per_job", 5))
        self.time_grouping: dict[str, float | int] | None = config.get("time_grouping")
        self.job = create_job(
            number_of_files=self.number_of_files,
            time_grouping=self.time_grouping,
            filters=self.filter,
        )

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
        if self.time_grouping is not None:
            file_date = getattr(file, "date", None)
            if file_date is None:
                _module_logger.error(
                    f"File {file} does not have a 'date' attribute; "
                    "cannot apply time grouping.",
                )
                return []  # return empty list if file doesn't have date attribute
            time_grouping_delta = timedelta(
                weeks=self.time_grouping.get("weeks", 0),
                hours=self.time_grouping.get("hours", 0),
                minutes=self.time_grouping.get("minutes", 0),
                seconds=self.time_grouping.get("seconds", 0),
            )
            start = str(self.time_grouping.get("start", "1900-01-01 00:00:00"))
            format_pattern = "%Y-%m-%d %H:%M:%S"
            start_datetime = datetime.strptime(start, format_pattern)

            time_group_id = int(
                (file_date.timestamp() - start_datetime.timestamp())
                // time_grouping_delta.total_seconds(),
            )
            return [str(time_group_id)]
        return super().get_job_ids_from_file(file)


class FilterAndGroupJobBuilder(JobBuilder):
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
    name: ClassVar[str] = "filter_pass"
    version: ClassVar[str] = "1"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
    ) -> None:
        """Initialize the DummyJobBuilder.

        Parameters
        ----------
        service : Service
            The service instance for the builder.
        config : dict
            Configuration dictionary for the builder.
        """
        super().__init__(service, config)
        if service is None or isinstance(service, types.ModuleType):
            return
        self._logger.debug(
            f"Initializing FilterAndGroupJobBuilder with config {config}",
        )
        self.config = config or {}
        self.job_groups = [FilterAndGroupJobGroup(self.config)]

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
        self._logger.debug("filter_pass handling incoming files")
        return super().handle_incoming_files()


PLUGIN_CLASS = FilterAndGroupJobBuilder
