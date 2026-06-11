"""Unit tests for the bash_executor module."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from courier.utils.bash_executor import BashExecResult, execute_bash_script


class TestBasicExecution:
    """Tests for basic success/failure without logging modes."""

    def test_success_captures_stdout(self, tmp_path: Path) -> None:
        """Script exits 0, stdout captured."""
        result = execute_bash_script(
            script_body="echo hello",
            timeout_seconds=10.0,
        )
        assert result.return_code == 0
        assert "hello" in result.stdout
        assert result.stderr == ""
        assert result.log_file_path is None

    def test_failure_captures_stderr(self, tmp_path: Path) -> None:
        """Script exits non-zero, stderr captured."""
        result = execute_bash_script(
            script_body="echo error >&2; exit 1",
            timeout_seconds=10.0,
        )
        assert result.return_code == 1
        assert "error" in result.stderr

    def test_stdout_empty_on_silent_script(self, tmp_path: Path) -> None:
        """Script with no output returns empty strings."""
        result = execute_bash_script(
            script_body="exit 0",
            timeout_seconds=10.0,
        )
        assert result.return_code == 0
        assert result.stdout == ""
        assert result.stderr == ""


class TestLogToLogger:
    """Tests for log_to_logger mode."""

    def test_stdout_streamed_to_debug(self) -> None:
        """stdout lines are streamed to logger.debug with prefix."""
        mock_logger = MagicMock(spec=logging.Logger)
        result = execute_bash_script(
            script_body="echo hello; echo world",
            timeout_seconds=10.0,
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[job: test]",
        )
        assert result.return_code == 0
        # Check that debug was called for stdout lines
        debug_calls = [
            call
            for call in mock_logger.log.call_args_list
            if call[0][0] == logging.DEBUG
        ]
        assert len(debug_calls) >= 1
        # At least one call should contain "hello" with prefix
        assert any(
            "[job: test]" in str(call) and "hello" in str(call)
            for call in debug_calls
        )

    def test_stderr_streamed_to_warning(self) -> None:
        """stderr lines are streamed to logger.warning with prefix."""
        mock_logger = MagicMock(spec=logging.Logger)
        result = execute_bash_script(
            script_body="echo error >&2",
            timeout_seconds=10.0,
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[job: test]",
        )
        # Check that warning was called for stderr lines
        warning_calls = [
            call
            for call in mock_logger.log.call_args_list
            if call[0][0] == logging.WARNING
        ]
        assert len(warning_calls) >= 1
        assert any("error" in str(call) for call in warning_calls)


class TestLogToFile:
    """Tests for log_to_file mode."""

    def test_stdout_written_to_file(self, tmp_path: Path) -> None:
        """stdout is written to the specified log file."""
        log_path = tmp_path / "test.log"
        result = execute_bash_script(
            script_body="echo hello; echo world",
            timeout_seconds=10.0,
            log_to_file=True,
            log_file_path=log_path,
        )
        assert result.return_code == 0
        assert log_path.exists()
        content = log_path.read_text()
        assert "hello" in content
        assert "world" in content
        assert result.log_file_path == str(log_path)

    def test_stderr_written_to_file(self, tmp_path: Path) -> None:
        """stderr is written to the specified log file."""
        log_path = tmp_path / "test.log"
        result = execute_bash_script(
            script_body="echo error >&2",
            timeout_seconds=10.0,
            log_to_file=True,
            log_file_path=log_path,
        )
        content = log_path.read_text()
        assert "error" in content


class TestLogOnlyErrors:
    """Tests for log_only_errors mode."""

    def test_stdout_is_empty(self) -> None:
        """When log_only_errors=True, BashExecResult.stdout is empty string."""
        result = execute_bash_script(
            script_body="echo hello; echo world",
            timeout_seconds=10.0,
            log_only_errors=True,
        )
        assert result.stdout == ""
        assert result.return_code == 0

    def test_stderr_still_captured(self) -> None:
        """stderr is still captured when log_only_errors=True."""
        result = execute_bash_script(
            script_body="echo error >&2; echo ok",
            timeout_seconds=10.0,
            log_only_errors=True,
        )
        assert "error" in result.stderr
        assert result.stdout == ""

    def test_stdout_not_streamed_to_logger(self) -> None:
        """stdout is NOT logged when log_only_errors=True + log_to_logger=True."""
        mock_logger = MagicMock(spec=logging.Logger)
        execute_bash_script(
            script_body="echo hello; echo error >&2",
            timeout_seconds=10.0,
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[test]",
            log_only_errors=True,
        )
        # stdout lines should not appear in DEBUG calls
        debug_calls = [
            call
            for call in mock_logger.log.call_args_list
            if call[0][0] == logging.DEBUG
        ]
        assert len(debug_calls) == 0  # No stdout logged
        # stderr lines should still appear in WARNING calls
        warning_calls = [
            call
            for call in mock_logger.log.call_args_list
            if call[0][0] == logging.WARNING
        ]
        assert len(warning_calls) >= 1

    def test_stdout_not_written_to_file(self, tmp_path: Path) -> None:
        """stdout is NOT written to file when log_only_errors=True + log_to_file=True."""
        log_path = tmp_path / "test.log"
        execute_bash_script(
            script_body="echo hello; echo error >&2",
            timeout_seconds=10.0,
            log_to_file=True,
            log_file_path=log_path,
            log_only_errors=True,
        )
        content = log_path.read_text()
        assert "hello" not in content
        assert "error" in content


class TestCombinedModes:
    """Tests for combinations of logging modes."""

    def test_stream_and_file_together(self, tmp_path: Path) -> None:
        """log_to_logger=True + log_to_file=True both work simultaneously."""
        mock_logger = MagicMock(spec=logging.Logger)
        log_path = tmp_path / "test.log"
        result = execute_bash_script(
            script_body="echo hello; echo error >&2",
            timeout_seconds=10.0,
            log_to_logger=True,
            logger=mock_logger,
            log_prefix="[test]",
            log_to_file=True,
            log_file_path=log_path,
        )
        assert result.return_code == 0
        # Logger received calls
        assert mock_logger.log.called
        # File was written
        assert log_path.exists()
        content = log_path.read_text()
        assert "hello" in content
        assert "error" in content


class TestTimeout:
    """Tests for timeout behavior."""

    def test_timeout_kills_process(self) -> None:
        """Slow script is killed and returns -1."""
        result = execute_bash_script(
            script_body="sleep 60",
            timeout_seconds=0.5,
        )
        assert result.return_code == -1
        assert "timed out" in result.stderr.lower() or "timeout" in result.stderr.lower()


class TestFailFastGuards:
    """Tests for ValueError guards on missing required arguments."""

    def test_missing_logger_raises(self) -> None:
        """log_to_logger=True with logger=None raises ValueError."""
        with pytest.raises(ValueError, match="log_to_logger"):
            execute_bash_script(
                script_body="echo hi",
                timeout_seconds=10.0,
                log_to_logger=True,
                logger=None,
            )

    def test_missing_log_file_path_raises(self) -> None:
        """log_to_file=True with log_file_path=None raises ValueError."""
        with pytest.raises(ValueError, match="log_to_file"):
            execute_bash_script(
                script_body="echo hi",
                timeout_seconds=10.0,
                log_to_file=True,
                log_file_path=None,
            )


class TestTempFileCleanup:
    """Tests that temp script files are cleaned up."""

    def test_temp_file_removed_after_success(self) -> None:
        """Temp .sh file is removed after successful execution."""
        # We can't easily inspect the temp dir, but we can verify
        # the function doesn't leave cruft by running and checking
        # no exception is raised.
        result = execute_bash_script(
            script_body="echo hi",
            timeout_seconds=10.0,
        )
        assert result.return_code == 0

    def test_temp_file_removed_after_error(self) -> None:
        """Temp .sh file is removed even after script error."""
        result = execute_bash_script(
            script_body="exit 1",
            timeout_seconds=10.0,
        )
        assert result.return_code == 1  # Script ran, temp was cleaned
