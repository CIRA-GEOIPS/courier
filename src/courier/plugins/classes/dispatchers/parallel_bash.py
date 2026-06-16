# ruff: noqa: E501
"""Parallel Bash Dispatcher — run one bash script per file concurrently.

Each file in the job triggers an independent script execution, with up to
``max_workers`` scripts running simultaneously via :class:`ThreadPoolExecutor`.
Failures are captured as :class:`ExecutionLog` entries (never raised); if
``fail_fast`` is enabled, the first non-zero return-code cancels all pending
submissions.
"""

from __future__ import annotations

import os
import socket
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import jinja2
from jinja2.exceptions import TemplateSyntaxError
from pydantic import BaseModel, Field, field_validator, model_validator

from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import DISPATCHER_PARALLEL_WORKERS_ACTIVE
from courier.dispatchers._output_file_pattern import (  # noqa: TC001
    OutputFilePattern,
)
from courier.dispatchers._output_scanner import (
    _scan_and_emit_output_files,
)
from courier.types.execution_log import ExecutionLog
from courier.utils.bash_executor import execute_bash_script

if TYPE_CHECKING:
    import logging as logging_module

    from courier.service import Service
    from courier.types.file import FrozenFile
    from courier.types.job import Job


class ParallelBashConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`ParallelBashDispatcher`."""

    bash_script: str
    max_workers: int = Field(default=4, ge=1, le=64)
    timeout_seconds: float = Field(default=3600.0, gt=0)
    fail_fast: bool = False
    log_to_logger: bool = Field(default=False)
    log_to_file: bool = Field(default=False)
    log_dir: str = Field(default="")
    log_only_errors: bool = Field(default=False)
    output_files: list[OutputFilePattern] | None = Field(default=None)
    scan_stderr: bool = Field(default=False)
    python_venv: str | None = Field(default=None)

    @model_validator(mode="after")
    def _validate_logging(self) -> ParallelBashConfig:
        if self.log_to_file and not self.log_dir:
            raise ValueError("log_dir is required when log_to_file=True")
        if self.log_to_file:
            log_path = Path(self.log_dir)
            if not log_path.is_dir():
                log_path.mkdir(parents=True, exist_ok=True)
        if self.log_to_file and self.log_dir:
            log_path = Path(self.log_dir)
            if not os.access(log_path, os.W_OK):
                raise ValueError(f"log_dir is not writable: {self.log_dir}")
        return self

    @field_validator("bash_script")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except TemplateSyntaxError as exc:
            raise ValueError(f"Invalid Jinja2 template: {exc}") from exc
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


def _run_script(  # noqa: PLR0913
    script_body: str,
    timeout_seconds: float,
    hostname: str,
    *,
    logger: logging_module.Logger | logging_module.LoggerAdapter | None = None,
    log_to_logger: bool = False,
    log_prefix: str = "",
    log_to_file: bool = False,
    log_file_path: Path | None = None,
    log_only_errors: bool = False,
    env: dict[str, str] | None = None,
) -> ExecutionLog:
    """Execute *script_body* under bash with configurable logging modes.

    Delegates to :func:`courier.utils.bash_executor.execute_bash_script`
    and maps the result to an :class:`ExecutionLog`.

    Never raises — all failure modes are captured in the returned ExecutionLog.
    """
    result = execute_bash_script(
        script_body=script_body,
        timeout_seconds=timeout_seconds,
        logger=logger,
        log_to_logger=log_to_logger,
        log_prefix=log_prefix,
        log_to_file=log_to_file,
        log_file_path=log_file_path,
        log_only_errors=log_only_errors,
        env=env,
    )
    return ExecutionLog(
        return_code=result.return_code,
        stdout=result.stdout,
        stderr=result.stderr,
        hostname=hostname,
        log_file_path=result.log_file_path,
    )


class ParallelBashDispatcher(Dispatcher):
    r"""Execute a Jinja2-templated bash script independently for each file.

    One script execution is launched per file in the job, up to
    ``max_workers`` concurrent scripts via :class:`ThreadPoolExecutor`.
    Each file execution produces its own :class:`ExecutionLog`.

    **Configuration** (:class:`ParallelBashConfig`):

    .. code-block:: yaml

        config:
          bash_script: |
            #!/bin/bash
            echo "Processing {{ file.file }}"
            cp {{ file.file }} /output/
          max_workers: 4
          timeout_seconds: 3600.0
          fail_fast: false

    **Template Context**

    Per-file context (each file gets its own template render):

    +------------------+---------------------------------------------+---------------------------+
    | Variable           | Description                                   | Example                    |
    +====================+===============================================+=============================+
    | ``file``            | Current file's :class:`FrozenFile` dict        | ``{{ file.file }}``        |
    |                    | (via :meth:`FrozenFile.to_dict`).              |                            |
    |                    | Keys: ``file``, ``hostname``, ``source``,      |                            |
    |                    | ``instrument``, ``processing_stage``,          |                            |
    |                    | ``domain``, ``num_expected``, ``timestamp``.   |                            |
    +--------------------+---------------------------------------------+----------------------------+
    | ``files``           | List of ALL files in the job as dicts          | ``{{ files[0].file }}``    |
    |                    | (for cross-reference/manifest generation).     |                            |
    +--------------------+---------------------------------------------+----------------------------+
    | ``job``             | Job metadata dict: ``name``, ``identifier``,    | ``{{ job.name }}``         |
    |                    | ``config``, ``last_modified``, ``timeout``,     |                             |
    |                    | ``correlation_id``, ``emit_time``.              |                             |
    +--------------------+---------------------------------------------+----------------------------+
    | ``config``          | Alias for ``job.config`` (convenience).         | ``{{ config.key }}``       |
    +--------------------+---------------------------------------------+----------------------------+

    **Error Handling**

    - **Config time**: Invalid Jinja2 syntax raises
      :class:`pydantic.ValidationError` at plugin registration.
    - **Render time**: Per-file template errors return
      ``ExecutionLog(return_code=-1, stderr=...)`` for that specific file —
      other files continue processing.
    - **Execution time**: Subprocess failures are captured as
      ``ExecutionLog`` entries with the process return code.
    - ``fail_fast=True`` cancels remaining files on first non-zero exit.

    **Jinja2 Undefined Behavior**

    Uses ``jinja2.DebugUndefined``. Simple undefined references render as
    empty strings; attribute access on undefined variables raises
    :exc:`jinja2.TemplateError`.

    **Serial vs Parallel**

    Use :class:`SerialBashDispatcher` when your script processes all files
    together (one invocation, one :class:`ExecutionLog`). Use
    :class:`ParallelBashDispatcher` when each file should be processed
    independently (N invocations, N :class:`ExecutionLog`\\s).
    """

    interface: ClassVar[str] = "dispatchers"
    family: ClassVar[str] = "standard"
    name: ClassVar[str] = "parallel_bash"
    version: ClassVar[str] = "0.1.0"

    def __init__(
        self,
        service: Service | types.ModuleType | None = None,
        config: dict[str, Any] | None = None,
        identifier: str | None = None,
    ) -> None:
        super().__init__(service, config, identifier=identifier)
        if service is None or isinstance(service, types.ModuleType):
            return
        self.validated = ParallelBashConfig.model_validate(config or {})
        self._template = jinja2.Environment(
            undefined=jinja2.DebugUndefined,
            autoescape=False,  # noqa: S701
        ).from_string(self.validated.bash_script)
        self._logger.debug(
            "Initialized ParallelBashDispatcher with config: "
            f"{self.validated.model_dump()}",
        )

    def is_healthy(self) -> bool:
        """Stateless dispatcher; always healthy when loaded."""
        return True

    def _render_script(
        self,
        ff: FrozenFile,
        job_context: dict,
        all_file_dicts: list[dict],
    ) -> str:
        """Render the Jinja2 bash template for a single FrozenFile with full job context."""
        context = {
            "file": ff.to_dict(),
            "files": all_file_dicts,
            "job": job_context,
            "config": job_context["config"],
        }
        return self._template.render(**context)

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Execute one script per file concurrently and return all logs."""
        hostname = socket.gethostname()
        frozen_files = [f for f in job.files if f.file is not None]
        if not frozen_files:
            self._logger.warning(f"Job {job.identifier} has no files to dispatch")
            return []

        all_file_dicts = [f.to_dict() for f in frozen_files]
        job_context = {
            "name": job.name,
            "identifier": job.identifier,
            "config": job.config,
            "last_modified": job.last_modified,
            "timeout": job.timeout,
            "correlation_id": job.correlation_id,
            "emit_time": job.emit_time,
        }

        self._logger.info(
            f"Dispatching {len(frozen_files)} files for job {job.identifier} "
            f"with max_workers={self.validated.max_workers}",
        )

        env: dict[str, str] | None = None
        if self.validated.python_venv:
            venv_path = Path(self.validated.python_venv)
            env = os.environ.copy()
            env["PATH"] = f"{venv_path / 'bin'}:{env.get('PATH', '')}"
            env["VIRTUAL_ENV"] = str(venv_path)

        logs: list[ExecutionLog] = []
        gauge = DISPATCHER_PARALLEL_WORKERS_ACTIVE.labels(
            dispatcher_name=self.name,
            dispatcher_identifier=self.identifier,
        )

        with ThreadPoolExecutor(max_workers=self.validated.max_workers) as pool:
            futures = {}
            for ff in frozen_files:
                try:
                    script_body = self._render_script(ff, job_context, all_file_dicts)
                except jinja2.TemplateError as exc:
                    self._logger.warning(
                        f"Template render failed for {ff.file}: {exc}",
                    )
                    logs.append(
                        ExecutionLog(
                            return_code=-1,
                            stdout="",
                            stderr=f"template render failed: {exc}",
                            hostname=hostname,
                        ),
                    )
                    continue
                log_prefix = (
                    f"[job: {job.identifier}] [file: {ff.file}]"
                    if self.validated.log_to_logger
                    else ""
                )
                log_path = None
                if self.validated.log_to_file:
                    ts = datetime.now().strftime("%Y%m%dT%H%M%S%f")
                    file_stem = Path(str(ff.file)).stem.replace(" ", "_")
                    log_path = (
                        Path(self.validated.log_dir)
                        / f"dispatch_{job.identifier}_{file_stem}_{ts}.log"
                    )
                futures[
                    pool.submit(
                        _run_script,
                        script_body,
                        self.validated.timeout_seconds,
                        hostname,
                        logger=self._logger if self.validated.log_to_logger else None,
                        log_to_logger=self.validated.log_to_logger,
                        log_prefix=log_prefix,
                        log_to_file=self.validated.log_to_file,
                        log_file_path=log_path if self.validated.log_to_file else None,
                        log_only_errors=self.validated.log_only_errors,
                        env=env,
                    )
                ] = ff.file

            gauge.set(len(futures))
            try:
                for future in as_completed(futures):
                    log = future.result()
                    logs.append(log)
                    if self.validated.fail_fast and (log.return_code or 0) != 0:
                        self._logger.warning(
                            f"fail_fast triggered by non-zero return "
                            f"{log.return_code}; cancelling remaining workers",
                        )
                        for pending in futures:
                            pending.cancel()
                        break
            finally:
                gauge.set(0)

        if self.validated.output_files:
            for log in logs:
                _scan_and_emit_output_files(
                    stdout=log.stdout or "",
                    stderr=log.stderr or "",
                    patterns=self.validated.output_files,
                    scan_stderr=self.validated.scan_stderr,
                    hostname=hostname,
                    emit_file=self.emit_file,
                )
        return logs


PLUGIN_CLASS = ParallelBashDispatcher
