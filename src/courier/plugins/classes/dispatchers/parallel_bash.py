"""Parallel Bash Dispatcher — run one bash script per file concurrently.

Each file in the job triggers an independent script execution, with up to
``max_workers`` scripts running simultaneously via :class:`ThreadPoolExecutor`.
Failures are captured as :class:`ExecutionLog` entries (never raised); if
``fail_fast`` is enabled, the first non-zero return-code cancels all pending
submissions.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import tempfile
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import jinja2
from jinja2.exceptions import TemplateSyntaxError
from pydantic import BaseModel, Field, field_validator

from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import DISPATCHER_PARALLEL_WORKERS_ACTIVE
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.file import FrozenFile
    from courier.types.job import Job


class ParallelBashConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`ParallelBashDispatcher`."""

    bash_script: str
    max_workers: int = Field(default=4, ge=1, le=64)
    timeout_seconds: float = Field(default=3600.0, gt=0)
    fail_fast: bool = False

    @field_validator("bash_script")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        try:
            jinja2.Environment(autoescape=False).parse(v)  # noqa: S701
        except TemplateSyntaxError as exc:
            raise ValueError(f"Invalid Jinja2 template: {exc}") from exc
        return v


def _run_script(
    script_body: str,
    timeout_seconds: float,
    hostname: str,
) -> ExecutionLog:
    """Write *script_body* to a temp file, exec under bash, return a log.

    Never raises — all failure modes collapse into ``return_code=-1``
    with diagnostic detail in ``stderr`` so the caller can aggregate
    logs without needing try/except around each future.
    """
    script_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sh",
            delete=False,
        ) as script_file:
            script_file.write(script_body)
            script_path = script_file.name
        Path(script_path).chmod(0o755)
        result = subprocess.run(  # noqa: S603
            ["/bin/bash", script_path],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return ExecutionLog(
            return_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            hostname=hostname,
        )
    except subprocess.TimeoutExpired as e:
        return ExecutionLog(
            return_code=-1,
            stdout=e.stdout.decode() if e.stdout else "",
            stderr=f"Script execution timed out after {timeout_seconds}s: {e!s}",
            hostname=hostname,
        )
    except (OSError, subprocess.SubprocessError) as e:
        return ExecutionLog(
            return_code=-1,
            stdout="",
            stderr=f"Error executing script: {e!s}",
            hostname=hostname,
        )
    finally:
        if script_path is not None:
            with contextlib.suppress(OSError):
                Path(script_path).unlink()


class ParallelBashDispatcher(Dispatcher):
    """Execute a Jinja2-templated bash script independently for each file.

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

    def _render_script(self, ff: FrozenFile, job_context: dict, all_file_dicts: list[dict]) -> str:
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

        logs: list[ExecutionLog] = []
        gauge = DISPATCHER_PARALLEL_WORKERS_ACTIVE.labels(dispatcher_name=self.name)

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
                futures[
                    pool.submit(
                        _run_script,
                        script_body,
                        self.validated.timeout_seconds,
                        hostname,
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

        return logs


PLUGIN_CLASS = ParallelBashDispatcher
