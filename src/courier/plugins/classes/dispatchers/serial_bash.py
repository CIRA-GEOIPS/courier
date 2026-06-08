"""Serial Bash Dispatcher Plugin for courier."""

from __future__ import annotations

import contextlib
import socket
import subprocess
import tempfile
import types
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import jinja2
from jinja2.exceptions import TemplateSyntaxError
from pydantic import BaseModel, Field, field_validator

from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job


class SerialBashConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`SerialBashDispatcher`."""

    bash_script: str
    timeout_seconds: float = Field(default=3600.0, gt=0)

    @field_validator("bash_script")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except TemplateSyntaxError as exc:
            raise ValueError(f"Invalid bash_script: {exc}") from exc
        return v


class SerialBashDispatcher(Dispatcher):
    r"""Execute a single Jinja2-templated bash script for an entire job.

    One script is rendered and executed per job — all files in the job are
    available in the template as a list. A single :class:`ExecutionLog` is
    returned (or none if the job has no files).

    **Configuration** (:class:`SerialBashConfig`):

    .. code-block:: yaml

        config:
          bash_script: |
            #!/bin/bash
            {% for f in files %}
            echo "Processing {{ f.file }}"
            cp {{ f.file }} /output/
            {% endfor %}

    **Template Context**

    The following variables are available inside the Jinja2 template:

    +------------------+---------------------------------------------+---------------------------+
    | Variable           | Description                                   | Example                    |
    +====================+===============================================+=============================+
    | ``files``           | List of all :class:`FrozenFile` dicts          | ``{{ files[0].file }}``    |
    |                    | (via :meth:`FrozenFile.to_dict`).              |                            |
    |                    | Each dict has keys: ``file``, ``hostname``,    |                            |
    |                    | ``source``, ``instrument``,                    |                            |
    |                    | ``processing_stage``, ``domain``,              |                            |
    |                    | ``num_expected``, ``timestamp``.               |                            |
    +--------------------+---------------------------------------------+----------------------------+
    | ``job``             | Job metadata dict: ``name``, ``identifier``,    | ``{{ job.name }}``         |
    |                    | ``config``, ``last_modified``, ``timeout``,     |                             |
    |                    | ``correlation_id``, ``emit_time``.              |                             |
    +--------------------+---------------------------------------------+----------------------------+
    | ``config``          | Alias for ``job.config`` (convenience).         | ``{{ config.key }}``       |
    +--------------------+---------------------------------------------+----------------------------+

    **Error Handling**

    - **Config time**: Invalid Jinja2 syntax in ``bash_script`` raises
      :class:`pydantic.ValidationError` at plugin registration (fail-fast).
    - **Render time**: Runtime template errors (e.g. accessing attributes on
      undefined variables) return ``ExecutionLog(return_code=-1, stderr=...)``
      — the pipeline continues.
    - **Execution time**: Subprocess timeouts and errors are captured as
      ``ExecutionLog`` entries (never raised).

    **Jinja2 Undefined Behavior**

    Uses ``jinja2.DebugUndefined`` as the undefined type. Simple undefined
    variable references (e.g. ``{{ missing }}``) render as an empty string.
    Attribute access on undefined variables
    (e.g. ``{{ missing.field }}``) raises :exc:`jinja2.TemplateError` at
    render time.

    **Serial vs Parallel**

    Use :class:`SerialBashDispatcher` when your script processes all files
    together (one invocation, one :class:`ExecutionLog`). Use
    :class:`ParallelBashDispatcher` when each file should be processed
    independently (N invocations, N :class:`ExecutionLog`\\s).
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "serial_bash"
    version: ClassVar[str] = "-1"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated = SerialBashConfig.model_validate(config or {})
        self._template = jinja2.Environment(
            undefined=jinja2.DebugUndefined,
            autoescape=False,  # noqa: S701
        ).from_string(self.validated.bash_script)
        self._logger.debug(
            "Initialized SerialBashDispatcher with config: "
            f"{self.validated.model_dump()}",
        )

    def is_healthy(self) -> bool:
        """Check if the dispatcher is healthy."""
        return True

    def _render_script(self, job: Job) -> str:
        """Render the Jinja2 bash template with job and config context."""
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
        return self._template.render(**context)

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Execute jobs serially as a subprocess and yield execution logs.

        Returns
        -------
            The log results of executing a processing workflow. Returns as a
            list of ExecutionLog objects.
        """
        self._logger.info(f"Executing job: {job}")

        if not job.files:
            self._logger.warning(
                f"No files in job {job.identifier}, nothing to execute",
            )
            return []

        hostname = socket.gethostname()
        try:
            script_content = self._render_script(job)
        except jinja2.TemplateError as exc:
            self._logger.exception("Template rendering failed")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"template render failed: {exc}",
                    hostname=hostname,
                ),
            ]

        self._logger.debug(f"Generated script content:\n{script_content}")
        script_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".sh",
                delete=False,
            ) as script_file:
                script_file.write(script_content)
                script_path = script_file.name

            Path(script_path).chmod(0o755)
            result = subprocess.run(  # noqa: S603
                ["/bin/bash", script_path],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.validated.timeout_seconds,
            )
            return [
                ExecutionLog(
                    return_code=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    hostname=hostname,
                ),
            ]
        except subprocess.TimeoutExpired as e:
            self._logger.exception("Script execution timed out")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout=e.stdout.decode() if e.stdout else "",
                    stderr=f"Script execution timed out: {e!s}",
                    hostname=hostname,
                ),
            ]
        except (OSError, subprocess.SubprocessError) as e:
            self._logger.exception("Error executing script")
            return [
                ExecutionLog(
                    return_code=-1,
                    stdout="",
                    stderr=f"Error executing script: {e!s}",
                    hostname=hostname,
                ),
            ]
        finally:
            if script_path is not None:
                with contextlib.suppress(OSError):
                    Path(script_path).unlink()


PLUGIN_CLASS = SerialBashDispatcher
