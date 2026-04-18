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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from courier.interfaces.module_based.dispatchers import Dispatcher
from courier.metrics import DISPATCHER_PARALLEL_WORKERS_ACTIVE
from courier.types.execution_log import ExecutionLog

if TYPE_CHECKING:
    from courier.service import Service
    from courier.types.job import Job

interface: str = "dispatchers"
family: str = "standard"
name: str = "parallel_bash"


class ParallelBashConfig(BaseModel, frozen=True):
    """Validated configuration for :class:`ParallelBashDispatcher`."""

    bash_script: str
    max_workers: int = Field(default=4, ge=1, le=64)
    timeout_seconds: float = Field(default=3600.0, gt=0)
    fail_fast: bool = False


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
    """Dispatcher that runs one bash script per file concurrently.

    Thread-safe: work is fanned out via :class:`ThreadPoolExecutor`; each
    worker writes to its own temp file and returns an :class:`ExecutionLog`.
    The active-worker gauge is incremented/decremented inside ``_submit``
    to reflect real concurrency.
    """

    interface = "dispatchers"
    name = "parallel_bash"
    version = "0.1.0"

    def __init__(self, service: Service, config: dict[str, Any]) -> None:
        super().__init__(service, config)
        self.validated = ParallelBashConfig.model_validate(config)
        self._logger.debug(
            f"Initialized ParallelBashDispatcher with config {self.validated}",
        )

    def is_healthy(self) -> bool:
        """Stateless dispatcher; always healthy when loaded."""
        return True

    def _render_script(self, file_path: Any) -> str:
        try:
            return self.validated.bash_script.format(file=file_path)
        except (KeyError, IndexError) as exc:
            self._logger.warning(
                f"Bash script template missing key {exc}; using raw template",
            )
            return self.validated.bash_script

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        """Execute one script per file concurrently and return all logs."""
        hostname = socket.gethostname()
        files = [f.file for f in job.files if f.file is not None]
        if not files:
            self._logger.warning(f"Job {job.identifier} has no files to dispatch")
            return []

        self._logger.info(
            f"Dispatching {len(files)} files for job {job.identifier} "
            f"with max_workers={self.validated.max_workers}",
        )

        logs: list[ExecutionLog] = []
        gauge = DISPATCHER_PARALLEL_WORKERS_ACTIVE.labels(dispatcher_name=self.name)

        with ThreadPoolExecutor(max_workers=self.validated.max_workers) as pool:
            futures = {
                pool.submit(
                    _run_script,
                    self._render_script(file_path),
                    self.validated.timeout_seconds,
                    hostname,
                ): file_path
                for file_path in files
            }
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


def call() -> None:
    """Raise error if called directly."""
    raise NotImplementedError("You cannot call this plugin directly.")
