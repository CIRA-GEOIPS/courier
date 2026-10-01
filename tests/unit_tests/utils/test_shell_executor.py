"""Behavioural tests for the shared shell executor.

``execute_shell_script`` is the single point where every shell payload's
process actually runs, so its logging modes, timeout handling and env
propagation are load-bearing. These tests run real subprocesses through
``/bin/bash -c`` rather than mocking ``Popen`` so the streaming threads,
log-file writes and process-group kill are exercised for real.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.utils.shell_executor import ShellExecResult, execute_shell_script


def _run(
    script: str, timeout_seconds: float = 10.0, **kwargs: object
) -> ShellExecResult:
    """Execute *script* through bash exactly as a shell payload would."""
    return execute_shell_script(["/bin/bash", "-c", script], timeout_seconds, **kwargs)  # type: ignore[arg-type]


class TestBasicExecution:
    """Success and failure without any logging modes enabled."""

    def test_success_captures_stdout(self) -> None:
        result = _run("echo hello")

        assert result.return_code == 0
        assert "hello" in result.stdout
        assert result.stderr == ""
        assert result.log_file_path is None

    def test_failure_captures_stderr(self) -> None:
        result = _run("echo error >&2; exit 1")

        assert result.return_code == 1
        assert "error" in result.stderr

    def test_silent_script_returns_empty_output(self) -> None:
        result = _run("exit 0")

        assert result.return_code == 0
        assert result.stdout == ""
        assert result.stderr == ""

    def test_env_is_propagated_to_the_process(self) -> None:
        result = _run("echo $COURIER_TEST_VAR", env={"COURIER_TEST_VAR": "propagated"})

        assert result.return_code == 0
        assert "propagated" in result.stdout

    def test_none_env_inherits_parent_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("COURIER_TEST_INHERITED", "inherited")

        result = _run("echo $COURIER_TEST_INHERITED", env=None)

        assert "inherited" in result.stdout


class TestLogToLogger:
    """Streaming output to a logger."""

    def test_stdout_streamed_at_debug(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)

        result = _run(
            "echo hello; echo world",
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[job: test]",
        )

        assert result.return_code == 0
        debug_calls = [
            c for c in mock_logger.log.call_args_list if c[0][0] == logging.DEBUG
        ]
        assert any("[job: test]" in str(c) and "hello" in str(c) for c in debug_calls)

    def test_stderr_streamed_at_warning(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)

        _run(
            "echo error >&2",
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[job: test]",
        )

        warning_calls = [
            c for c in mock_logger.log.call_args_list if c[0][0] == logging.WARNING
        ]
        assert any("error" in str(c) for c in warning_calls)


class TestLogToFile:
    """Persisting output to a log file."""

    def test_stdout_and_stderr_written_to_file(self, tmp_path: Path) -> None:
        log_path = tmp_path / "test.log"

        result = _run(
            "echo hello; echo error >&2",
            log_to_file=True,
            log_file_path=log_path,
        )

        assert result.return_code == 0
        content = log_path.read_text()
        assert "hello" in content
        assert "error" in content
        assert result.log_file_path == str(log_path)


class TestLogOnlyErrors:
    """``log_only_errors`` must suppress stdout everywhere."""

    def test_only_stderr_is_captured(self) -> None:
        result = _run("echo error >&2; echo ok", log_only_errors=True)

        assert result.return_code == 0
        assert result.stdout == ""
        assert "error" in result.stderr

    def test_stdout_is_not_streamed_to_logger(self) -> None:
        mock_logger = MagicMock(spec=logging.Logger)

        _run(
            "echo hello; echo error >&2",
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[test]",
            log_only_errors=True,
        )

        debug_calls = [
            c for c in mock_logger.log.call_args_list if c[0][0] == logging.DEBUG
        ]
        warning_calls = [
            c for c in mock_logger.log.call_args_list if c[0][0] == logging.WARNING
        ]
        assert debug_calls == []
        assert len(warning_calls) >= 1

    def test_stdout_is_not_written_to_file(self, tmp_path: Path) -> None:
        log_path = tmp_path / "test.log"

        _run(
            "echo hello; echo error >&2",
            log_to_file=True,
            log_file_path=log_path,
            log_only_errors=True,
        )

        content = log_path.read_text()
        assert "hello" not in content
        assert "error" in content


class TestCombinedModes:
    def test_stream_and_file_together(self, tmp_path: Path) -> None:
        mock_logger = MagicMock(spec=logging.Logger)
        log_path = tmp_path / "test.log"

        result = _run(
            "echo hello; echo error >&2",
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[test]",
            log_to_file=True,
            log_file_path=log_path,
        )

        assert result.return_code == 0
        assert mock_logger.log.called
        content = log_path.read_text()
        assert "hello" in content
        assert "error" in content


class TestTimeout:
    def test_slow_script_is_killed_and_reported(self) -> None:
        result = _run("sleep 60", timeout_seconds=0.5)

        assert result.return_code == -1
        assert "timed out" in result.stderr.lower()


class TestFailFastGuards:
    def test_log_to_logger_without_logger_raises(self) -> None:
        with pytest.raises(ValueError, match="log_to_logger"):
            _run("echo hi", log_to_logger=True, logger=None)

    def test_log_to_file_without_path_raises(self) -> None:
        with pytest.raises(ValueError, match="log_to_file"):
            _run("echo hi", log_to_file=True, log_file_path=None)


class TestCommandsThatCannotStart:
    """A command that cannot start is a failed result, never an exception."""

    def test_missing_executable_is_reported(self) -> None:
        result = execute_shell_script(["/nonexistent/courier-no-such-binary"], 5.0)

        assert result.return_code == -1
        assert "Error executing script" in result.stderr

    def test_embedded_nul_byte_is_reported(self) -> None:
        result = execute_shell_script(["/bin/echo", "bad\x00arg"], 5.0)

        assert result.return_code == -1
        assert "Error executing script" in result.stderr

    def test_none_in_argv_is_reported(self) -> None:
        result = execute_shell_script([None, "-c", "true"], 5.0)  # type: ignore[list-item]

        assert result.return_code == -1
        assert "Error executing script" in result.stderr

    def test_log_file_that_cannot_be_opened_is_reported(self, tmp_path: Path) -> None:
        log_path = tmp_path / "missing-dir" / "run.log"

        result = _run("echo hi", log_to_file=True, log_file_path=log_path)

        assert result.return_code == -1
        assert "Error executing script" in result.stderr
        assert result.log_file_path is None


class TestReportedLogFile:
    def test_log_file_is_not_reported_when_file_logging_is_off(
        self, tmp_path: Path
    ) -> None:
        log_path = tmp_path / "unused.log"

        result = _run("echo hi", log_file_path=log_path)

        assert result.return_code == 0
        assert result.log_file_path is None
        assert not log_path.exists()
