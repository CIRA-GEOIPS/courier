"""Unit tests for the serial_bash dispatcher plugin (Jinja2-based)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pydantic
import pytest

from courier.plugins.classes.dispatchers.serial_bash import (
    SerialBashConfig,
    SerialBashDispatcher,
)
from courier.types.execution_log import ExecutionLog
from courier.utils.bash_executor import BashExecResult


def _make_config(**overrides):
    cfg = {"bash_script": "echo {{ files[0].file }}"}
    cfg.update(overrides)
    return cfg


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_stores_config(self, mock_service: MagicMock) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        assert isinstance(plugin.validated, SerialBashConfig)
        assert plugin.validated.bash_script == "echo {{ files[0].file }}"

    def test_missing_bash_script_raises(self, mock_service: MagicMock) -> None:
        with pytest.raises(pydantic.ValidationError):
            SerialBashDispatcher(mock_service, {}, identifier="test-disp")

    def test_invalid_jinja_syntax_raises(self, mock_service: MagicMock) -> None:
        with pytest.raises(pydantic.ValidationError):
            SerialBashDispatcher(
                mock_service,
                _make_config(bash_script="{% if %}"),
                identifier="test-disp",
            )


# ─── is_healthy ─────────────────────────────────────────────────────────────


class TestIsHealthy:
    def test_always_healthy(self, mock_service: MagicMock) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        assert plugin.is_healthy() is True


# ─── get_execution_log ──────────────────────────────────────────────────────


class TestGetExecutionLog:
    def test_success_returns_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_bash_exec_result,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        execute = mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=fake_bash_exec_result(return_code=0, stdout="hi"),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)

        assert len(logs) == 1
        assert isinstance(logs[0], ExecutionLog)
        assert logs[0].return_code == 0
        assert logs[0].stdout == "hi"

        # The rendered script is the thing that actually runs on a cluster;
        # asserting only that execute_bash_script was called would pass with
        # an unrendered template or the wrong file substituted in.
        execute.assert_called_once()
        assert execute.call_args.kwargs["script_body"] == "echo /tmp/a.nc"

    def test_empty_files_returns_empty(
        self,
        mock_service: MagicMock,
        make_job,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        job = make_job(files=())
        logs = plugin.get_execution_log(job)
        assert logs == []

    def test_template_render_error_returns_failure(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
    ) -> None:
        # Accessing .foo on an undefined variable triggers an attribute
        # lookup on the DebugUndefined proxy, which raises TemplateError.
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(bash_script="{{ undefined_var.foo }}"),
            identifier="test-disp",
        )
        job = make_job(files=(make_frozen_file(),))
        logs = plugin.get_execution_log(job)

        assert len(logs) == 1
        assert isinstance(logs[0], ExecutionLog)
        assert logs[0].return_code == -1
        assert "template render failed" in (logs[0].stderr or "")

    def test_timeout_returns_failure_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_bash_exec_result,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=fake_bash_exec_result(
                return_code=-1,
                stderr="Script execution timed out after 1.0s",
            ),
        )
        job = make_job(files=(make_frozen_file(),))
        logs = plugin.get_execution_log(job)
        assert logs[0].return_code == -1
        assert "timed out" in (logs[0].stderr or "")

    def test_subprocess_error_returns_failure_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_bash_exec_result,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=fake_bash_exec_result(
                return_code=-1,
                stderr="Error executing script: boom",
            ),
        )
        job = make_job(files=(make_frozen_file(),))
        logs = plugin.get_execution_log(job)
        assert logs[0].return_code == -1
        assert "boom" in (logs[0].stderr or "")

    def test_script_file_cleaned_up(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_bash_exec_result,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        captured_body: list[str] = []

        def fake_execute(script_body, **__):
            captured_body.append(script_body)
            return fake_bash_exec_result()

        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            side_effect=fake_execute,
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/single.nc")),))
        plugin.get_execution_log(job)
        assert len(captured_body) == 1
        # Non-empty is not enough: an unrendered template is also non-empty.
        assert captured_body[0] == "echo /tmp/single.nc"
        assert "{{" not in captured_body[0], "template left unrendered"

    def test_multi_file_rendering(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_bash_exec_result,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(
                bash_script="{% for f in files %}echo {{ f.file }};{% endfor %}",
            ),
            identifier="test-disp",
        )
        captured_body: list[str] = []

        def fake_execute(script_body, **__):
            captured_body.append(script_body)
            return fake_bash_exec_result()

        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            side_effect=fake_execute,
        )
        job = make_job(
            files=(
                make_frozen_file(file=Path("/tmp/a.nc")),
                make_frozen_file(file=Path("/tmp/b.nc")),
                make_frozen_file(file=Path("/tmp/c.nc")),
            ),
        )
        plugin.get_execution_log(job)

        assert len(captured_body) == 1
        script = captured_body[0]
        assert "/tmp/a.nc" in script
        assert "/tmp/b.nc" in script
        assert "/tmp/c.nc" in script


# ─── Logging Modes ───────────────────────────────────────────────────────────


class TestLogToLogger:
    """Tests for log_to_logger=True mode in SerialBashDispatcher."""

    def test_stdout_streamed_to_logger_debug(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """Script stdout is streamed to self._logger.debug in real-time."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_to_logger=True),
            identifier="test-disp",
        )
        mock_exec = mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=0, stdout="hello\n", stderr="", log_file_path=None,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        plugin.get_execution_log(job)
        assert mock_exec.call_args[1]["logger"] is plugin._logger
        assert mock_exec.call_args[1]["log_to_logger"] is True

    def test_stderr_streamed_to_logger_warning(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """Script stderr is streamed to self._logger.warning in real-time."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_to_logger=True),
            identifier="test-disp",
        )
        mock_exec = mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=1, stdout="", stderr="error msg\n", log_file_path=None,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        plugin.get_execution_log(job)
        assert mock_exec.call_args[1]["logger"] is plugin._logger
        assert mock_exec.call_args[1]["log_to_logger"] is True


class TestLogToFile:
    """Tests for log_to_file=True mode in SerialBashDispatcher."""

    def test_log_file_created_with_content(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """Log file is created and contains script output."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_to_file=True, log_dir=str(log_dir)),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=0,
                stdout="processed\n",
                stderr="",
                log_file_path=str(log_dir / "dispatch_test_20260101T000000000000.log"),
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert len(logs) == 1
        assert logs[0].log_file_path is not None

    def test_log_file_path_in_execution_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """ExecutionLog.log_file_path is set when log_to_file=True."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_to_file=True, log_dir=str(log_dir)),
            identifier="test-disp",
        )
        expected_path = str(log_dir / "dispatch_test_20260101T000000000000.log")
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=0,
                stdout="ok",
                stderr="",
                log_file_path=expected_path,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert logs[0].log_file_path == expected_path


class TestLogOnlyErrors:
    """Tests for log_only_errors=True mode in SerialBashDispatcher."""

    def test_stdout_is_empty_in_execution_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """ExecutionLog.stdout is empty when log_only_errors=True."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_only_errors=True),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=0, stdout="", stderr="", log_file_path=None,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert logs[0].stdout == ""

    def test_stderr_still_captured(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """ExecutionLog.stderr is still captured when log_only_errors=True."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_only_errors=True),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=1, stdout="", stderr="error occurred", log_file_path=None,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert logs[0].stderr == "error occurred"
        assert logs[0].stdout == ""


class TestCombinedModes:
    """Tests for combinations of logging modes."""

    def test_stream_and_file_together(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """log_to_logger + log_to_file both active simultaneously."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(
                log_to_logger=True,
                log_to_file=True,
                log_dir=str(log_dir),
            ),
            identifier="test-disp",
        )
        mocker.patch.object(plugin._logger, "debug")
        mocker.patch.object(plugin._logger, "warning")
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=0,
                stdout="ok",
                stderr="",
                log_file_path=str(log_dir / "dispatch_test_20260101T000000000000.log"),
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert len(logs) == 1
        assert logs[0].log_file_path is not None
        # Logger should have been used
        assert plugin._logger.debug.called or plugin._logger.warning.called


class TestLogFileOnError:
    """Tests for log file creation on execution errors."""

    def test_log_file_set_on_timeout(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """Timeout errors still produce an ExecutionLog with log_file_path set."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(log_to_file=True, log_dir=str(log_dir)),
            identifier="test-disp",
        )
        expected_path = str(log_dir / "dispatch_test_timeout.log")
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script",
            return_value=BashExecResult(
                return_code=-1,
                stdout="",
                stderr="timed out",
                log_file_path=expected_path,
            ),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert logs[0].return_code == -1
        assert logs[0].log_file_path == expected_path


# ─── Python Venv ─────────────────────────────────────────────────────────────


class TestPythonVenvConfigValidation:
    """Tests for python_venv config field validation."""

    def test_python_venv_none_is_valid(self) -> None:
        """Config with python_venv=None passes validation."""
        cfg = SerialBashConfig(bash_script="echo hello", python_venv=None)
        assert cfg.python_venv is None

    def test_python_venv_omitted_is_valid(self, mock_service: MagicMock) -> None:
        """Config that omits python_venv entirely passes validation."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(),
            identifier="test-disp",
        )
        assert plugin.validated.python_venv is None

    def test_python_venv_path_not_a_directory(self, tmp_path: Path) -> None:
        """A regular file path for python_venv raises ValidationError."""
        file_path = tmp_path / "regular_file"
        file_path.write_text("not a directory")
        with pytest.raises(pydantic.ValidationError):
            SerialBashConfig(
                bash_script="echo hello", python_venv=str(file_path),
            )

    def test_python_venv_path_missing(self, tmp_path: Path) -> None:
        """A non-existent path for python_venv raises ValidationError."""
        missing = tmp_path / "does_not_exist"
        with pytest.raises(pydantic.ValidationError):
            SerialBashConfig(
                bash_script="echo hello", python_venv=str(missing),
            )

    def test_python_venv_no_bin_python(self, tmp_path: Path) -> None:
        """A directory without bin/python inside raises ValidationError."""
        venv_dir = tmp_path / "fake_venv"
        venv_dir.mkdir()
        with pytest.raises(pydantic.ValidationError):
            SerialBashConfig(
                bash_script="echo hello", python_venv=str(venv_dir),
            )

    def test_python_venv_valid_path_accepted(self) -> None:
        """sys.prefix (which has bin/python) passes validation.

        The stored value must be resolved to an absolute path.
        """
        cfg = SerialBashConfig(bash_script="echo hello", python_venv=sys.prefix)
        stored = cfg.python_venv
        assert stored is not None
        stored_path = Path(stored)
        assert stored_path.is_absolute()
        assert stored_path.is_dir()
        assert (stored_path / "bin" / "python").is_file()

    def test_python_venv_relative_path_resolved(
        self, tmp_path: Path,
    ) -> None:
        """A relative python_venv path is resolved to absolute by the validator."""
        venv_dir = tmp_path / "my_venv"
        venv_dir.mkdir()
        bin_dir = venv_dir / "bin"
        bin_dir.mkdir()
        python_bin = bin_dir / "python"
        python_bin.write_text("fake python")
        python_bin.chmod(0o755)

        original_cwd = Path.cwd()
        os.chdir(tmp_path)
        try:
            cfg = SerialBashConfig(
                bash_script="echo hello", python_venv="./my_venv",
            )
            assert cfg.python_venv is not None
            assert Path(cfg.python_venv).is_absolute()
        finally:
            os.chdir(original_cwd)


class TestPythonVenvEnvPropagation:
    """Tests for python_venv environment variable propagation to executor."""

    def test_python_venv_env_passed_to_executor(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
    ) -> None:
        """When python_venv is set, env dict with VIRTUAL_ENV and PATH is passed."""
        venv_path = sys.prefix
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(python_venv=venv_path),
            identifier="test-disp",
        )
        target = "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script"
        with patch(target, return_value=BashExecResult(
            return_code=0, stdout="ok", stderr="",
        )) as mock_exec:
            job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
            plugin.get_execution_log(job)

        mock_exec.assert_called_once()
        call_kwargs = mock_exec.call_args[1]
        assert "env" in call_kwargs
        env = call_kwargs["env"]
        assert isinstance(env, dict)
        assert "VIRTUAL_ENV" in env
        assert env["PATH"].startswith(str(Path(venv_path) / "bin"))

    def test_python_venv_not_set_env_is_none(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
    ) -> None:
        """When python_venv is not set, execute_bash_script receives env=None."""
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(python_venv=None),
            identifier="test-disp",
        )
        target = "courier.plugins.classes.dispatchers.serial_bash.execute_bash_script"
        with patch(target, return_value=BashExecResult(
            return_code=0, stdout="ok", stderr="",
        )) as mock_exec:
            job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
            plugin.get_execution_log(job)

        mock_exec.assert_called_once()
        call_kwargs = mock_exec.call_args[1]
        assert call_kwargs["env"] is None
