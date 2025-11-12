from geoips_driver.interfaces.module_based.data_monitors import File
from geoips_driver.interfaces.module_based.job_queuers import JobGroup, JobReady
from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging("OVERCAST-Job-Queuer")

interface = None


class OVERCASTJobGroup(JobGroup):
    def __init__(self, config) -> None:
        super().__init__("OVERCAST-Job-Group", config)

    def file_is_relevant(self, file):
        return True

    def get_job_id_from_file(self, file: File):
        return super().get_job_id_from_file(file)


class OVERCASTJobQueuer(JobReady):
    name = "OVERCAST-Job-Queuer"
    version = "-1"

    def __init__(self, service):
        super().__init__(service)
        logger.debug("Starting job queuer init function")

    def initialize(self, config):
        super().initialize(config)
        self.config = config["plugin_config"]
        self.job_groups = [OVERCASTJobGroup(self.config)]

    def is_healthy(self):
        return True
