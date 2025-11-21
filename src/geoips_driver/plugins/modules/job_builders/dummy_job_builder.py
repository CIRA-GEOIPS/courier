from geoips_driver.interfaces.module_based.data_monitors import File
from geoips_driver.interfaces.module_based.job_builders import Job, JobBuilder, JobGroup
from geoips_driver.interfaces.module_based.service import Service, setup_logging

logger = setup_logging("dummy_job_builder")

interface = None


class DummyJob(Job):
    def ready(self):
        return True  # if len(self.files) > 0 else False

    def add_file(self, file: File) -> None:
        if len(self.files) >= 1:
            logger.warning(
                "DummyJob: Maximum number of files reached; cannot add more.",
            )
            return
        return super().add_file(file)


class DummyJobGroup(JobGroup):
    def __init__(self, config) -> None:
        super().__init__("DummyJob", config)
        self.job = DummyJob

    def file_is_relevant(self, file):
        return True

    def get_job_id_from_file(self, file: File):
        return super().get_job_id_from_file(file)


class DummyJobBuilder(JobBuilder):
    name = "DummyJobBuilder"
    version = "-1"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        logger.debug(f"Initializing DummyJobBuilder with config {config}")
        self.config = config
        self.job_groups = [DummyJobGroup(self.config)]

    def is_healthy(self) -> bool:
        """Return health status."""
        return True

    def handle_incoming_files(self) -> None:
        """Listen to incoming files and mark job as ready when appropriate."""
        logger.debug("DummyJobBuilder handling incoming files")
        return super().handle_incoming_files()
