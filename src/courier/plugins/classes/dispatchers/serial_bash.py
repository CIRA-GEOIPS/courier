"""Serial Bash Dispatcher Plugin for courier."""

from __future__ import annotations

import os
import re as _re
import socket
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import jinja2
from jinja2.exceptions import TemplateSyntaxError
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field, field_validator, model_validator

from courier.dispatchers._output_file_pattern import (  # noqa: TC001
    OutputFilePattern,
)
from courier.dispatchers._output_scanner import (
    _scan_and_emit_output_files,
)
from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import COURIER_CUSTOM_GAUGE
from courier.tracing import (
    ATTR_CORRELATION_ID,
    ATTR_EXECUTION_RETURN_CODE,
    ATTR_JOB_ID,
    get_tracer,
)
from courier.types.execution_log import ExecutionLog
from courier.utils.bash_executor import BashExecResult, execute_bash_script
from courier.utils.functional import slugify_for_filename
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job


#: Module logger for the stdout-metric conduit, which is a module-level
#: function with no access to a plugin instance logger.
_logger = get_logger("module", "serial_bash", None)

#: Regex that extracts ``metric_name`` and ``value`` from a
#: ``COURIER_METRIC: <name> <value>`` stdout line.
#: The value pattern is deliberately permissive; :func:`_ingest_courier_metrics`
#: validates it with ``float()`` and skips anything that fails.
_COURIER_METRIC_RE = _re.compile(
    r"^COURIER_METRIC:\s+(?P<metric_name>\S+)\s+(?P<value>-?[\d.e+-]+)"
)


def _ingest_courier_metrics(
    stdout: str,
    dispatcher_identifier: str,
) -> None:
    """Scan *stdout* for ``COURIER_METRIC:`` lines and update Prometheus.

    This is a general-purpose conduit: deployment bash scripts emit custom
    Prometheus gauge values by printing::

        COURIER_METRIC: <metric_name> <numeric_value>

    The dispatcher recognises the prefix after every job execution and
    pushes the value into ``courier_custom_gauge`` with labels
    ``dispatcher_identifier`` and ``metric_name``.
    """
    for line in stdout.splitlines():
        m = _COURIER_METRIC_RE.match(line.strip())
        if not m:
            continue
        raw_value = m.group("value")
        try:
            value = float(raw_value)
        except ValueError:
            # The value pattern admits strings float() rejects ("1.2.3", "5--",
            # "1e"). This runs inside get_execution_log, so an escaping
            # ValueError is not a CourierError, is not caught by the dispatcher
            # loop, and takes the process down via os._exit(1) -- with the
            # message unacked and therefore redelivered, killing the service
            # again on restart. A bad metric line must never do that.
            _logger.warning(
                "Ignoring malformed COURIER_METRIC value %r for metric %r",
                raw_value,
                m.group("metric_name"),
            )
            continue
        COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier=dispatcher_identifier,
            metric_name=m.group("metric_name"),
        ).set(value)


class SerialBashConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`SerialBashDispatcher`."""

    bash_script: str
    timeout_seconds: float = Field(default=3600.0, gt=0)
    log_to_logger: bool = Field(default=False)
    log_to_file: bool = Field(default=False)
    log_dir: str = Field(default="")
    log_only_errors: bool = Field(default=False)
    output_files: list[OutputFilePattern] | None = Field(default=None)
    scan_stderr: bool = Field(default=False)
    python_venv: str | None = Field(default=None)

    @field_validator("bash_script")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except TemplateSyntaxError as exc:
            raise ValueError(f"Invalid bash_script: {exc}") from exc
        return v

    @field_validator("python_venv")
    @classmethod
    def _validate_python_venv(cls, v: str | None) -> str | None:
        if v is None:
            return v
        venv_path = Path(v).resolve()
        if not venv_path.is_dir():
            raise ValueError(
                f"python_venv is not a directory: {v}",
            )
        python_bin = venv_path / "bin" / "python"
        if not python_bin.is_file():
            raise ValueError(
                f"python_venv has no bin/python executable: {v}",
            )
        return str(venv_path)

    @model_validator(mode="after")
    def _validate_logging_config(self) -> SerialBashConfig:
        if self.log_to_file and not self.log_dir:
            raise ValueError("log_dir is required when log_to_file=True")
        if self.log_to_file:
            log_dir_path = Path(self.log_dir)
            if not log_dir_path.is_dir():
                log_dir_path.mkdir(parents=True, exist_ok=True)
            elif not os.access(self.log_dir, os.W_OK):
                raise ValueError(f"log_dir is not writable: {self.log_dir}")
        return self


SerialBashConfig.model_rebuild()


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
        service: Service,
        config: dict | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
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
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "dispatcher.execute_job",
            attributes={
                ATTR_JOB_ID: job.identifier,
                ATTR_CORRELATION_ID: job.correlation_id,
            },
        ) as span:
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
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute(ATTR_EXECUTION_RETURN_CODE, -1)
                return [
                    ExecutionLog(
                        return_code=-1,
                        stdout="",
                        stderr=f"template render failed: {exc}",
                        hostname=hostname,
                    ),
                ]

            self._logger.debug(f"Generated script content:\n{script_content}")

            log_file_path: Path | None = None
            if self.validated.log_to_file:
                ts = datetime.now().strftime("%Y%m%dT%H%M%S%f")
                safe_id = slugify_for_filename(job.identifier)
                log_file_path = (
                    Path(self.validated.log_dir) / f"dispatch_{safe_id}_{ts}.log"
                )

            log_prefix = f"[job: {job.identifier}]" if self.validated.log_to_logger else ""

            env: dict[str, str] | None = None
            if self.validated.python_venv:
                venv_path = Path(self.validated.python_venv)
                env = os.environ.copy()
                env["PATH"] = f"{venv_path / 'bin'}:{env.get('PATH', '')}"
                env["VIRTUAL_ENV"] = str(venv_path)

            result: BashExecResult = execute_bash_script(
                script_body=script_content,
                timeout_seconds=self.validated.timeout_seconds,
                logger=self._logger if self.validated.log_to_logger else None,
                log_to_logger=self.validated.log_to_logger,
                log_prefix=log_prefix,
                log_to_file=self.validated.log_to_file,
                log_file_path=log_file_path,
                log_only_errors=self.validated.log_only_errors,
                env=env,
            )
            span.set_attribute(
                ATTR_EXECUTION_RETURN_CODE, result.return_code,
            )
            if result.return_code != 0:
                span.set_status(Status(StatusCode.ERROR))
            if self.validated.output_files:
                _scan_and_emit_output_files(
                    stdout=result.stdout,
                    stderr=result.stderr,
                    patterns=self.validated.output_files,
                    scan_stderr=self.validated.scan_stderr,
                    hostname=hostname,
                    emit_file=self.emit_file,
                )
            _ingest_courier_metrics(result.stdout, self.identifier)
            return [
                ExecutionLog(
                    return_code=result.return_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    hostname=hostname,
                    log_file_path=result.log_file_path,
                ),
            ]

