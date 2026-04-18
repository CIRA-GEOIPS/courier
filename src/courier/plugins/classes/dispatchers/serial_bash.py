"""Serial Bash Dispatcher Plugin for courier."""

from __future__ import annotations

import socket
import subprocess
import tempfile
import types
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job


class SerialBashDispatcher(Dispatcher):
    """Serial Bash Dispatcher Plugin."""

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "serial_bash"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
    ) -> None:
        super().__init__(service, config)
        if service is None or isinstance(service, types.ModuleType):
            return
        config = config or {}
        self.config = config
        self.bash_script = config["bash_script"]
        self._logger.debug(
            f"Initialized SerialBashDispatcher with config: {self.config}",
        )

    def is_healthy(self) -> bool:
        """Check if the dispatcher is healthy."""
        return True

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Execute jobs serially as a subprocess and yield execution logs.

        Returns
        -------
            The log results of executing a processing workflow. Returns as a
            list of ExecutionLog objects.
        """
        self._logger.info(f"Executing job: {job}")

        # Create a temporary file for the bash script
        script_content = self.bash_script.format(file=next(iter(job.files)).file)
        self._logger.debug(f"Generated script content:\n{script_content}")
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
            self._logger.exception("Script execution timed out")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout=e.stdout.decode() if e.stdout else "",
                    stderr=f"Script execution timed out: {e!s}",
                    hostname=socket.gethostname(),
                ),
            ]
        except (OSError, subprocess.SubprocessError) as e:
            self._logger.exception("Error executing script")
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
            except OSError as e:
                self._logger.warning(f"Failed to delete temporary script file: {e}")


PLUGIN_CLASS = SerialBashDispatcher
