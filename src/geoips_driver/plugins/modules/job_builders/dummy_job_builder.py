from geoips_driver.interfaces.module_based.data_monitors import File
from geoips_driver.interfaces.module_based.job_builders import JobBuilder, JobGroup
from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging("dummy_job_builder")

interface = None


class DummyJobGroup(JobGroup):
    def __init__(self, config) -> None:
        super().__init__("OVERCAST-Job-Group", config)

    def file_is_relevant(self, file):
        return True

    def get_job_id_from_file(self, file: File):
        return super().get_job_id_from_file(file)


class DummyJobBuilder(JobBuilder):
    name = "DummyJobBuilder"
    version = "-1"

    def __init__(self, service):
        super().__init__(service)
        logger.debug("Starting job builder init function")

    def initialize(self, config):
        # super().initialize(config)
        self.config = config
        self.job_groups = [DummyJobGroup(self.config)]

    def is_healthy(self):
        return True
