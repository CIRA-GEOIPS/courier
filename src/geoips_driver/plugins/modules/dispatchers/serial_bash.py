"""Serial Bash Dispatcher Plugin for GeoIPS Driver."""

import socket
import subprocess
import tempfile
from pathlib import Path

from geoips_driver.interfaces.module_based.dispatchers import (
    Dispatcher,
    ExecutionLog,
)
from geoips_driver.interfaces.module_based.job_builders import Job
from geoips_driver.interfaces.module_based.service import Service, setup_logging

logger = setup_logging()

interface: str = "dispatchers"
family: str = "standard"
name = "serial_bash"


class SerialBashDispatcher(Dispatcher):
    """Serial Bash Dispatcher Plugin."""

    interface = "dispatchers"

    name = "serial_bash"
    version = "-1"

    def __init__(self, service: Service, config: dict) -> None:
        super().__init__(service, config)
        self.config = config
        self.bash_script = config["bash_script"]
        logger.debug(f"Initialized SerialBashDispatcher with config: {self.config}")

    def is_healthy(self) -> bool:
        """Check if the dispatcher is healthy."""
        return True

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Execute jobs serially as a subprocess and yield execution logs.

        Returns
        -------
            The log results of executing a GeoIPS processing workflow. Returns as a
            list of ExecutionLog objects.
        """
        logger.info(f"Executing job: {job}")

        # Create a temporary file for the bash script
        script_content = self.bash_script.format(file=next(iter(job.files)).file)
        logger.debug(f"Generated script content:\n{script_content}")
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sh",
            delete=False,
        ) as script_file:
            script_file.write(script_content)
            script_path = script_file.name

        try:
            # Make the script executable
            Path(script_path).chmod(0o755)

            # Execute the bash script
            result = subprocess.run(  # noqa: S603
                ["/bin/bash", script_path],
                check=False,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout, adjust as needed
            )

            return [
                ExecutionLog(
                    return_code=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    hostname=socket.gethostname(),
                ),
            ]

        except subprocess.TimeoutExpired as e:
            logger.exception("Script execution timed out")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout=e.stdout.decode() if e.stdout else "",
                    stderr=f"Script execution timed out: {e!s}",
                    hostname=socket.gethostname(),
                ),
            ]
        except Exception as e:
            logger.exception("Error executing script")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"Error executing script: {e!s}",
                    hostname=socket.gethostname(),
                ),
            ]
        finally:
            # Clean up the temporary script file
            try:
                Path(script_path).unlink()
            except Exception as e:
                logger.warning(f"Failed to delete temporary script file: {e}")


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")
