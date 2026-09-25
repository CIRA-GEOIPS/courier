"""Implementation for the base falconer class."""

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import jinja2

from courier.constants import PluginRunState
from courier.errors import CourierError
from courier.interfaces.discovery import (
    ENTRY_POINT_PREFIX,
    ClassPluginRegistry,
)
from courier.interfaces.falcons import DispatcherGroupConfig, Falcon
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.metrics import FALCONER_JOBS_PROCESSED, collect_labeled
from courier.service import Service
from courier.tracing import ATTR_CORRELATION_ID, ATTR_JOB_ID, get_tracer
from courier.types.execution_log import ExecutionLog
from courier.types.job import Job
from courier.utils.logging import get_logger


@dataclass
class FalconerPayload:
    """Environment payload for falcon cast-off."""

    command: list[str]
    file: Path
    log_prefix: str = ""
    log_file_path: Path | None = None


class Falconer(ServicePlugin):
    """Base class for the falconer plugin type."""

    interface: ClassVar[str] = "falconers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "falconer"

    representations: list[type[Falcon]]
    falcon: Falcon
    base_config: DispatcherGroupConfig

    def __init__(
        self,
        service: Service,
        config: dict | None = None,  # noqa: ARG002
        identifier: str | None = None,
    ) -> None:
        if identifier is None:
            raise ValueError(
                f"Falconer {type(self).__name__} requires an identifier",
            )
        self._logger = get_logger("plugin", self.name, service.config)
        self.parent_service = service
        self._state: PluginRunState
        self.identifier = identifier
        self.representations = []

        self._jobs_processed = FALCONER_JOBS_PROCESSED

    def cast_off_falcon(self, job: Job) -> list[ExecutionLog]:
        """Initialize the runtime environment and execute the Falcon.

        Parameters
        ----------
        job : Job
            The job to prepare and execute.

        Returns
        -------
        list[ExecutionLog]
            Execution logs produced by the Falcon.

        Raises
        ------
        CourierError
            If the Falconer fails to initialize the runtime environment.
        """
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "falconer.cast_off_falcon",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ):
            self._state = PluginRunState.RUNNING
            # ----------initialize environment
            try:
                p = self.initialize_environment(job)
            except Exception as e:
                self._state = PluginRunState.FAILED
                raise CourierError(
                    f"Failed to initialize environment for {self.identifier}",
                    e,
                ) from e
            # -------------get payload
            try:
                self._logger.debug(f"Yielding execution log for job: {job}")
                res = self.falcon.get_payload_from_job(p.command, job)

                status = (
                    "failure" if any(r.return_code != 0 for r in res) else "success"
                )
                self._jobs_processed.labels(
                    status=status,
                    falconer_name=self.name,
                    falconer_identifier=self.identifier,
                ).inc()

                p.file.unlink(missing_ok=True)

            except Exception as e:
                self._state = PluginRunState.FAILED
                self._jobs_processed.labels(
                    status="failure",
                    falconer_name=self.name,
                    falconer_identifier=self.identifier,
                ).inc()
                raise CourierError(
                    "Failed to execute job",
                    e,
                ) from e

            return res

    def initialize_environment(self, job) -> FalconerPayload:  # noqa: ARG002
        """Set up the runtime environment.

        Usually this means creating an array of commands.

        Parameters
        ----------
        job : Job
            The job whose runtime environment should be initialized.

        Returns
        -------
        FalconerPayload
            The command and associated metadata required to execute the job.
        """
        return FalconerPayload(command=[])

    def get_metrics(self) -> dict[str, Any]:
        """Return Falconer-specific metrics.

        Returns
        -------
        dict[str, Any]
            Mapping of metric names to their current values.
        """
        return {
            **collect_labeled(FALCONER_JOBS_PROCESSED, "falconer_name", self.name),
        }

    def set_falcon(self, falcon: Falcon):
        """Associate a Falcon with this Falconer.

        Parameters
        ----------
        falcon : Falcon
            The Falcon that will execute commands prepared by this Falconer.
        """
        self.falcon = falcon

    def _render_script_file(self, job: Job, path: Path | None = None) -> Path:
        """Render the script against a jinja2 template.

        Redirect to a directory if necessary.

        Parameters
        ----------
        job : Job
            The job whose values are used to render the script template.
        path : Path | None, optional
            Directory in which to create the rendered script. If ``None``, the
            system temporary directory is used.

        Returns
        -------
        Path
            Path to the rendered executable script.

        Raises
        ------
        CourierError
        If the source script cannot be read, rendered, written, or made
        executable.
        """
        if path is not None:
            try:
                path.mkdir(parents=True, exist_ok=True)
            except Exception:
                self._logger.exception(
                    f"Failed to create directory for temporary script at {path}",
                )
                raise
        try:
            falcon_config = self.falcon.config
            rendered_script_path: str | None = None

            script = falcon_config.file.read_text()
            rendered_script = self.render_script(job, script)

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=falcon_config.file.suffix,
                dir=path or "/tmp/",  # noqa: S108
                delete=False,
            ) as script_file:
                script_file.write(rendered_script)
                rendered_script_path = script_file.name
            res = Path(rendered_script_path)
            res.chmod(0o755)
            return res  # noqa: TRY300
        except Exception as e:
            raise CourierError(
                f"Failed to render script file for {self.identifier}",
                e,
            ) from e

    def _generate_execution_command(
        self,
        job: Job,
        path: Path | None = None,
    ) -> list[str]:
        """Generate the command to be executed by the falcon at runtime.

        Parameters
        ----------
        job : Job
            The job whose values are used to render command arguments.
        path : Path | None, optional
            Path supplied to the Falcon when declaring its execution command.

        Returns
        -------
        list[str]
        Rendered command arguments ready for execution.
        """
        raw_command = self.falcon.declare_command(path)
        command_arr = []

        for command in raw_command:
            command_arr.append(self.render_script(job, command))

        self._logger.debug(f"Falconer generated command: {' '.join(command_arr)}")
        return command_arr

    def render_script(self, job: Job, script: str) -> str:
        """Render the Jinja2 bash template with job and config context.

        Parameters
        ----------
        job : Job
            The job supplying template values and file metadata.
        script : str
            Jinja2 template to render.

        Returns
        -------
        str
        The rendered script or command.
        """
        context = {
            "files": [
                f.to_dict() for f in sorted(job.files, key=lambda f: str(f.file))
            ],
            "job": {
                "name": job.name,
                "identifier": job.identifier,
                "config": job.config,
                "last_modified": job.last_modified,
                "timeout": job.timeout,
                "correlation_id": job.correlation_id,
                "emit_time": job.emit_time,
            },
            "config": job.config,
        }
        return (
            jinja2.Environment(
                undefined=jinja2.StrictUndefined,
                autoescape=False,  # noqa: S701
                finalize=lambda value: "" if value is None or value == [] else value,
            )
            .from_string(script)
            .render(**context)
        )

    def _validate_toolchain(self):
        """Validate the provided falcon toolchain.

        Validate the provided falcon toolchain against its runtime environment using
        the falcon's set method.

        Raises
        ------
        CourierError
        If a required tool cannot be validated, validation returns no
        execution logs, or the validation command exits unsuccessfully.
        """
        self._logger.debug(f"Toolchain validation started for {self.identifier}")
        for value in self.falcon.config.toolchain:
            payload = self.falcon.validate_toolchain_arg(value)
            if len(payload) > 0:
                if payload[0].return_code != 0:
                    self._state = PluginRunState.FAILED
                    raise CourierError(
                        f"Toolchain validation failed for value {value}"
                        f" on falconer {self.identifier}",
                        payload[0].stderr,
                    )
                else:
                    self._logger.info(
                        f"Toolchain validation succeeded for value {value}",
                    )
            else:
                self._state = PluginRunState.FAILED
                raise CourierError(
                    "Toolchain validation failed with no warning.",
                )

    def start(self) -> None:
        """Check if the environment is suitable and wait.

        Raises
        ------
        CourierError
            If one or more required runtime tools cannot be validated.
        """
        self._validate_toolchain()
        self._state = PluginRunState.STOPPED

    def stop(self) -> None:
        """Stop the Falconer."""
        return

    def is_healthy(self) -> bool:
        """Return whether the Falconer is currently healthy.

        Returns
        -------
        bool
            ``True`` when the Falconer is running, otherwise ``False``.
        """
        return self._state != PluginRunState.FAILED


falconers = ClassPluginRegistry(
    name="falconers",
    group=f"{ENTRY_POINT_PREFIX}.falconers",
    expected_base=Falconer,
)
