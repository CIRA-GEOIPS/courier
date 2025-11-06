from geoips_driver.interfaces.module_based.data_monitors import File
from geoips_driver.interfaces.module_based.job_queuer import JobGroup, JobReady
from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging("JoberGroup")


class JoberGroup(JobGroup):
    def __init__(self, config) -> None:
        super().__init__("DummyJober", config)

    def file_is_relevant(file):
        return True

    def get_job_id_from_file(self, file: File):
        return super().get_job_id_from_file(file)


class JoberQueuer(JobReady):
    name = "Dummy-Jober"
    version = "-1"

    def __init__(self, service):
        super().__init__(service)
        logger.debug("Starting job queuer init function")

    def initialize(self, config):
        self.config = config
        self.job_groups = [JoberGroup(config)]

    def is_healthy(self):
        return True
