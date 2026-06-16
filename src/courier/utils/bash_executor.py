"""Shared helper for executing bash scripts with configurable logging modes.

Provides :func:`execute_bash_script` — a single entry-point that runs a bash
script body and captures stdout/stderr, optionally streaming to a logger and/or
a log file in real-time. The function **never raises**; all failure modes are
captured in the returned :class:`BashExecResult`.
"""

from __future__ import annotations

import contextlib
import logging
import subprocess
import tempfile
import threading
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import cast


@dataclass
class BashExecResult:
    """Result of executing a bash script.

    Attributes
    ----------
    return_code : int
        Process exit code. -1 indicates a timeout or internal error.
    stdout : str
        Captured standard output (empty string if ``log_only_errors`` is True).
    stderr : str
        Captured standard error output.
    log_file_path : str or None
        Path to the log file if ``log_to_file`` was True, else None.
    """

    return_code: int
    stdout: str
    stderr: str
    log_file_path: str | None = None


def execute_bash_script(  # noqa: PLR0913, PLR0915
    script_body: str,
    timeout_seconds: float,
    *,
    logger: logging.Logger | logging.LoggerAdapter | None = None,
    log_to_logger: bool = False,
    log_prefix: str = "",
    log_to_file: bool = False,
    log_file_path: Path | None = None,
    log_only_errors: bool = False,
    env: dict[str, str] | None = None,
) -> BashExecResult:
    """Execute a bash script with configurable logging modes.

    Writes *script_body* to a temporary file, executes it via ``/bin/bash``,
    and captures stdout/stderr. Depending on flags, output may be streamed
    to a logger and/or written to a file in real-time.

    Never raises — all failure modes are captured in :class:`BashExecResult`.

    Parameters
    ----------
    script_body : str
        The bash script content to execute.
    timeout_seconds : float
        Maximum execution time before kill.
    logger : logging.Logger or logging.LoggerAdapter or None
        Logger or adapter for streaming output (required if ``log_to_logger=True``).
    log_to_logger : bool
        Stream stdout→DEBUG, stderr→WARNING to the logger in real-time.
    log_prefix : str
        Prefix prepended to each log line (e.g., ``"[job: abc] [file: /data/x.nc]"``).
    log_to_file : bool
        Write stdout and stderr to a file in real-time.
    log_file_path : Path or None
        Path to the log file (required if ``log_to_file=True``).
    log_only_errors : bool
        If True, discard stdout entirely (not streamed, not written to file,
        not included in returned ``BashExecResult.stdout``).
    env : dict[str, str] or None
        Environment variables for the subprocess. If ``None``, the parent
        process's ``os.environ`` is inherited (existing behavior).

    Returns
    -------
    BashExecResult
        Result dataclass with ``return_code``, ``stdout``, ``stderr``,
        ``log_file_path``.

    Raises
    ------
    ValueError
        If ``log_to_logger=True`` but *logger* is None, or ``log_to_file=True``
        but *log_file_path* is None.
    """
    # -- Fail-fast: validate required args for enabled modes ------------------
    if log_to_logger and logger is None:
        raise ValueError("log_to_logger is True but no logger was provided")
    if log_to_file and log_file_path is None:
        raise ValueError("log_to_file is True but no log_file_path was provided")

    script_path: str | None = None
    log_fh = None
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    try:
        # -- Write script body to a temporary file ----------------------------
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sh",
            delete=False,
        ) as script_file:
            script_file.write(script_body)
            script_path = script_file.name

        Path(script_path).chmod(0o755)

        # -- Open log file if requested ---------------------------------------
        if log_to_file:
            log_fh = cast("Path", log_file_path).open("w")
            log_fh.write("# Dispatch log — script started\n")
            log_fh.flush()

        # -- Launch subprocess with piped stdout/stderr -----------------------
        process = subprocess.Popen(  # noqa: S603
            ["/bin/bash", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # -- Threaded readers for stdout and stderr ---------------------------
        def _read_stream(
            stream: typing.IO[str],
            lines_list: list[str],
            level: int,
            stream_name: str,
        ) -> None:
            """Read lines from *stream* and dispatch to logger/file/accumulator."""
            for line in iter(stream.readline, ""):
                if log_only_errors and stream_name == "stdout":
                    continue
                lines_list.append(line)
                _log_line(line, level, stream_name)
                _write_to_file(line, stream_name)

        def _log_line(line: str, level: int, stream_name: str) -> None:
            """Deliver a single line to the logger if requested."""
            if not log_to_logger or logger is None:
                return
            if log_only_errors and stream_name == "stdout":
                return
            logger.log(level, f"{log_prefix} [{stream_name}] {line.rstrip()}")

        def _write_to_file(line: str, stream_name: str) -> None:
            """Append a single line to the log file if open."""
            if log_fh is None:
                return
            if log_only_errors and stream_name == "stdout":
                return
            log_fh.write(f"[{stream_name}] {line}")
            log_fh.flush()

        stdout_thread = threading.Thread(
            target=_read_stream,
            args=(process.stdout, stdout_lines, logging.DEBUG, "stdout"),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_read_stream,
            args=(process.stderr, stderr_lines, logging.WARNING, "stderr"),
            daemon=True,
        )

        stdout_thread.start()
        stderr_thread.start()

        # -- Wait for process completion (with timeout) -----------------------
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            return_code = -1
            stderr_lines.append(
                f"Script execution timed out after {timeout_seconds}s\n",
            )
        finally:
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

        return BashExecResult(
            return_code=return_code,
            stdout="".join(stdout_lines),
            stderr="".join(stderr_lines),
            log_file_path=str(log_file_path) if log_file_path else None,
        )

    except (OSError, subprocess.SubprocessError) as e:
        return BashExecResult(
            return_code=-1,
            stdout="",
            stderr=f"Error executing script: {e!s}",
            log_file_path=str(log_file_path) if log_file_path else None,
        )

    finally:
        if log_fh is not None:
            log_fh.close()
        if script_path is not None:
            with contextlib.suppress(OSError):
                Path(script_path).unlink()
