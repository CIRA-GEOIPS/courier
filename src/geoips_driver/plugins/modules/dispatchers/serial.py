import json
from typing import Generator

from geoips_driver.interfaces.module_based.dispatchers import Dispatcher
from geoips_driver.interfaces.module_based.dispatchers import ExecutionLog
from geoips_driver.interfaces.module_based.job_queuers import JOB_READY_QUEUE
from geoips_driver.interfaces.module_based.service import setup_logging
from geoips_driver.utils.driver_components import process_utils, template_utils
from geoips_driver.utils.generate_workflow import generate_workflow_from_steps

logger = setup_logging()


class SerialDispatcher(Dispatcher):
    name = "Serial-Dispatcher"
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
        # self.template = (
        #     config.get("dispatcher", {}).get("arguments", {}).get("template")
        # )
        # self.template_dir = config.get("template_dir", None)
        # self.steps = config.get("dispatcher", {}).get("arguments", {}).get("steps", {})

        # NOTE: UNCOMMENT CODE ABOVE ONCE READY TO DYNAMICALLY LOAD CONFIG FILE

        self.template = "order_based_template"
        self.template_dir = None
        self.steps = {
            "workflow": {
                "kind": "workflow",
                "name": "abi_infrared",
                "arguments": {},
            },
            "output_formatter": {
                "kind": "output_formatter",
                "name": "imagery_annotated",
                "arguments": {"sectors": ["goes_east"]},
            },
        }

    def is_healthy(self):
        return True

    def yield_execution_log(self) -> Generator[ExecutionLog, None, None]:
        """
        Listens for messages from DataMonitor plugins and queries the GeoIPS db.

        Yields:
            log: The log results of executing a GeoIPS processing workflow.
        """
        template = template_utils.get_template(
            self.template, template_dir=self.template_dir
        )
        generated_workflow = generate_workflow_from_steps(self.steps)

        message_generator = self.parent_service.consume(JOB_READY_QUEUE)
        logger.info(f"Listening for RabbitMQ messages from the Database Data Monitor.")

        try:
            while True:
                for message in message_generator:
                    logger.info(f"INCOMING MESSAGE = {message}")

                    if message:
                        fpaths = []
                        # Add all of the filepaths to a single list for now. We will
                        # deal with data_fusion based approaches later on.
                        # We expect the type of message to be a set in this instance
                        for fpath in message:
                            fpaths.append(fpath)
                        # for sat_sensor, filepaths in message.get(
                        #     "sensor_filepath_mapping", {}
                        # ).items():
                        #     fpaths += filepaths

                        raw_workflow = json.dumps(generated_workflow)
                        bash_script = template.render(
                            {"filepaths": " ".join(fpaths), "generated": raw_workflow}
                        )
                        try:
                            result = process_utils.run_temp_script(bash_script)
                            yield ExecutionLog(
                                return_code=result.returncode,
                                stdout=result.stdout,
                                stderr=result.stderr,
                                hostname="localhost",
                            )
                        except Exception as e:
                            logger.warning(str(e))

        except KeyboardInterrupt:
            logger.warning(
                "[Serial Dispatcher]: Keyboard Interrupt occurred. Stopping..."
            )
            self.stop()
        finally:
            message_generator.close()
