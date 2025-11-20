from collections.abc import Generator

from geoips_driver.interfaces.module_based.dispatchers import Dispatcher, ExecutionLog
from geoips_driver.interfaces.module_based.job_builders import JOB_READY_QUEUE
from geoips_driver.interfaces.module_based.service import setup_logging

logger = setup_logging()

interface = None


class SerialDispatcher(Dispatcher):
    name = "serial_bash_dispatcher"
    interface = "dispatchers"
    version = "-1"

    def __init__(self, service):
        super().__init__(service)

    def initialize(self, config):
        """Initialize the starting arguments for the serial dispatcher..

        Parameters
        ----------
        template: str
            - The name of the template we are going to use to produce GeoIPS bash
            scripts
        steps: SimpleNamespace
            - A SimpleNamespace object representing an ordered dictionary which depicts
            the order of operations needed to produce the correct output
        template_dir: str, default=None
            - The path to the directory which contains 'template'. If None, this
            defaults to $GEOIPS_PACKAGES_DIR/geoips_driver/geoips_driver/templates
        """
        self.config = config

    def is_healthy(self):
        return True

    def yield_execution_log(self) -> Generator[ExecutionLog, None, None]:
        """
        Listens for messages from DataMonitor plugins and queries the GeoIPS db.

        Yields
        ------
            log: The log results of executing a GeoIPS processing workflow.
        """
        message_generator = self.parent_service.consume(JOB_READY_QUEUE)

        try:
            while True:
                for message in message_generator:
                    yield ExecutionLog(
                        return_code=-1,
                        stdout="stdout",
                        stderr="stderr",
                        hostname="localhost",
                    )
        finally:
            message_generator.close()
