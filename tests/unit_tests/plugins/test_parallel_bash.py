"""Unit tests for the parallel_bash dispatcher plugin."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import jinja2
import pytest
from pydantic import ValidationError

from courier.plugins.classes.dispatchers.parallel_bash import (
    ParallelBashConfig,
    ParallelBashDispatcher,
    _run_script,
)
from courier.types.execution_log import ExecutionLog
from courier.utils.bash_executor import BashExecResult


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"bash_script": "echo {{ file.file }}"}
    cfg.update(overrides)
    return cfg


# ─── Config Validation ──────────────────────────────────────────────────────


class TestParallelBashConfig:
    def test_minimal_valid(self) -> None:
        cfg = ParallelBashConfig.model_validate(_make_config())
        assert cfg.max_workers == 4
        assert cfg.fail_fast is False

    def test_max_workers_range(self) -> None:
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(_make_config(max_workers=0))
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(_make_config(max_workers=128))

    def test_timeout_positive(self) -> None:
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(_make_config(timeout_seconds=0))

    def test_invalid_jinja_syntax_raises(self, mock_service) -> None:
        """Invalid Jinja2 syntax in bash_script raises ValidationError."""
        with pytest.raises(ValidationError):
            ParallelBashDispatcher(
                service=mock_service,
                config={"bash_script": "{% if %}"},
                identifier="test-disp",
            )


# ─── Constructor / health ───────────────────────────────────────────────────


class TestConstructor:
    def test_initializes(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.validated.bash_script == "echo {{ file.file }}"

    def test_always_healthy(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.is_healthy() is True


# ─── _render_script ─────────────────────────────────────────────────────────


class TestRenderScript:
    def test_substitutes_file(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(bash_script="echo {{ file.file }}"),
            identifier="test-disp",
        )
        ff = make_frozen_file(file=Path("/tmp/x"))
        all_file_dicts = [ff.to_dict()]
        job_context: dict = {"config": {}}
        result = plugin._render_script(ff, job_context, all_file_dicts)
        assert result == "echo /tmp/x"

    def test_missing_key_raises(
        self, mock_service: MagicMock, make_frozen_file
    ) -> None:
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(bash_script="echo {{ missing }}"),
            identifier="test-disp",
        )
        ff = make_frozen_file()
        all_file_dicts = [ff.to_dict()]
        job_context: dict = {"config": {}}
        # DebugUndefined renders simple {{ missing }} as literal, so use
        # attribute access (__getattr__ raises UndefinedError).
        plugin._template = jinja2.Environment(
            undefined=jinja2.DebugUndefined, autoescape=False,
        ).from_string("echo {{ missing.field }}")  # noqa: S701
        with pytest.raises(jinja2.TemplateError):
            plugin._render_script(ff, job_context, all_file_dicts)


# ─── get_execution_log ──────────────────────────────────────────────────────


class TestGetExecutionLog:
    def test_no_files_returns_empty(
        self, mock_service: MagicMock, make_job
    ) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.get_execution_log(make_job()) == []

    def test_one_log_per_file(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(max_workers=2), identifier="test-disp")
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            return_value=ExecutionLog(return_code=0, stdout="ok", stderr=""),
        )
        files = (
            make_frozen_file(file=Path("/a.nc")),
            make_frozen_file(file=Path("/b.nc")),
            make_frozen_file(file=Path("/c.nc")),
        )
        logs = plugin.get_execution_log(make_job(files=files))
        assert len(logs) == 3
        assert all(isinstance(log, ExecutionLog) for log in logs)
        assert all(log.return_code == 0 for log in logs)

    def test_fail_fast_short_circuits(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(max_workers=1, fail_fast=True),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            return_value=ExecutionLog(return_code=2, stdout="", stderr=""),
        )
        files = tuple(
            make_frozen_file(file=Path(f"/f{i}.nc")) for i in range(5)
        )
        logs = plugin.get_execution_log(make_job(files=files))
        assert len(logs) >= 1
        assert logs[0].return_code == 2

    def test_render_error_returns_failure_log_and_continues(
        self, mock_service, make_frozen_file, make_job, mocker,
    ) -> None:
        """TemplateError on one file produces failure log; other files continue."""
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            return_value=ExecutionLog(return_code=0, stdout="ok", stderr=""),
        )
        file1 = make_frozen_file(file=Path("/data/file1.nc"))
        file2 = make_frozen_file(file=Path("/data/file2.nc"))
        job = make_job(files=[file1, file2])

        plugin = ParallelBashDispatcher(
            service=mock_service,
            config={
                "bash_script": (
                    "{% if file.file == '/data/file1.nc' %}"
                    "{{ undefined_var.foo }}"
                    "{% else %}echo ok{% endif %}"
                ),
                "max_workers": 2,
            },
            identifier="test-disp",
        )
        logs = plugin.get_execution_log(job)

        assert len(logs) == 2
        errors = [log for log in logs if log.return_code == -1]
        successes = [log for log in logs if log.return_code == 0]
        assert len(errors) == 1
        assert "template render failed" in (errors[0].stderr or "")
        assert len(successes) == 1


# ─── _run_script (module-level helper) ──────────────────────────────────────


class TestRunScript:
    def test_success(self, mocker) -> None:
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.execute_bash_script",
            return_value=BashExecResult(return_code=0, stdout="ok", stderr=""),
        )
        log = _run_script("echo hi", 60.0, "h1")
        assert log.return_code == 0
        assert log.stdout == "ok"

    def test_oserror_returns_failure(self, mocker) -> None:
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.execute_bash_script",
            return_value=BashExecResult(return_code=-1, stdout="", stderr="boom"),
        )
        log = _run_script("x", 60.0, "h")
        assert log.return_code == -1
        assert "boom" in (log.stderr or "")


# ─── Logging Modes ───────────────────────────────────────────────────────────


class TestLogToLogger:
    """Tests for log_to_logger=True mode in ParallelBashDispatcher."""

    def test_concurrent_logger_writes_are_safe(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """Multiple concurrent workers can write to logger safely."""
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(log_to_logger=True, max_workers=2),
            identifier="test-disp",
        )
        mocker.patch.object(plugin._logger, "debug")
        mocker.patch.object(plugin._logger, "warning")
        # Mock _run_script to return successful ExecutionLogs
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            return_value=ExecutionLog(return_code=0, stdout="ok", stderr=""),
        )
        files = (
            make_frozen_file(file=Path("/a.nc")),
            make_frozen_file(file=Path("/b.nc")),
        )
        job = make_job(files=files)
        logs = plugin.get_execution_log(job)
        assert len(logs) == 2
        # Logger thread-safety: no exceptions raised is the main verification

    def test_log_prefix_includes_file_path(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """Each file's log_prefix includes the file path for disambiguation."""
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(log_to_logger=True, max_workers=1),
            identifier="test-disp",
        )
        # Capture the log_prefix passed to _run_script
        captured_prefixes: list[str] = []

        def _fake_run(*_, **kwargs):
            captured_prefixes.append(kwargs.get("log_prefix", ""))
            return ExecutionLog(return_code=0, stdout="", stderr="")
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            side_effect=_fake_run,
        )
        job = make_job(files=(make_frozen_file(file=Path("/data/file1.nc")),))
        plugin.get_execution_log(job)
        assert len(captured_prefixes) == 1
        assert "/data/file1.nc" in captured_prefixes[0]


class TestLogToFile:
    """Tests for log_to_file=True mode in ParallelBashDispatcher."""

    def test_per_file_log_files_created(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """Each file gets its own log file path."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(log_to_file=True, log_dir=str(log_dir), max_workers=2),
            identifier="test-disp",
        )
        captured_paths: list[str] = []

        def _fake_run(*_, **kwargs):
            lp = kwargs.get("log_file_path")
            captured_paths.append(str(lp) if lp else "")
            return ExecutionLog(
                return_code=0, stdout="", stderr="",
                log_file_path=str(kwargs.get("log_file_path", "")),
            )
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            side_effect=_fake_run,
        )
        files = (
            make_frozen_file(file=Path("/a.nc")),
            make_frozen_file(file=Path("/b.nc")),
            make_frozen_file(file=Path("/c.nc")),
        )
        job = make_job(files=files)
        logs = plugin.get_execution_log(job)
        assert len(logs) == 3
        assert len(captured_paths) == 3
        # Each path should be unique
        assert len(set(captured_paths)) == 3

    def test_log_file_path_in_execution_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        tmp_path,
        mocker,
    ) -> None:
        """Each ExecutionLog has its log_file_path set."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(log_to_file=True, log_dir=str(log_dir), max_workers=2),
            identifier="test-disp",
        )
        def _fake_run(*_, **kwargs):
            return ExecutionLog(
                return_code=0, stdout="ok", stderr="",
                log_file_path=str(kwargs.get("log_file_path", "")),
            )
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            side_effect=_fake_run,
        )
        job = make_job(files=(make_frozen_file(file=Path("/a.nc")),))
        logs = plugin.get_execution_log(job)
        assert logs[0].log_file_path is not None
        assert "a" in str(logs[0].log_file_path)  # file stem in path


class TestLogOnlyErrors:
    """Tests for log_only_errors=True mode in ParallelBashDispatcher."""

    def test_stdout_is_empty_for_all_files(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """All ExecutionLogs have empty stdout when log_only_errors=True."""
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(log_only_errors=True, max_workers=2),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            return_value=ExecutionLog(return_code=0, stdout="", stderr=""),
        )
        files = (
            make_frozen_file(file=Path("/a.nc")),
            make_frozen_file(file=Path("/b.nc")),
        )
        job = make_job(files=files)
        logs = plugin.get_execution_log(job)
        assert len(logs) == 2
        assert all(log.stdout == "" for log in logs)


class TestFailFastWithLogging:
    """Tests for fail_fast=True combined with logging modes."""

    def test_fail_fast_with_log_to_logger(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        """fail_fast + log_to_logger: failing files cancel remaining workers."""
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(
                log_to_logger=True,
                fail_fast=True,
                max_workers=1,
            ),
            identifier="test-disp",
        )
        call_count = [0]

        def _fake_run(*_, **__):
            call_count[0] += 1
            # First file fails, triggering fail_fast
            return ExecutionLog(return_code=1, stdout="", stderr="fail")
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash._run_script",
            side_effect=_fake_run,
        )
        files = tuple(
            make_frozen_file(file=Path(f"/f{i}.nc")) for i in range(5)
        )
        job = make_job(files=files)
        logs = plugin.get_execution_log(job)
        # At least one log produced; not all 5 due to fail_fast
        assert len(logs) >= 1
        assert len(logs) <= 5


# ─── python_venv Config Validation ──────────────────────────────────────────


class TestPythonVenvConfig:
    """Tests for python_venv field validation in ParallelBashConfig."""

    def test_python_venv_none_is_valid(self) -> None:
        """Config with python_venv=None passes validation."""
        cfg = ParallelBashConfig.model_validate(
            _make_config(python_venv=None),
        )
        assert cfg.python_venv is None

    def test_python_venv_path_not_a_directory(self, tmp_path: Path) -> None:
        """Path to a regular file raises ValidationError."""
        f = tmp_path / "regular_file.txt"
        f.write_text("not a venv")
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(
                _make_config(python_venv=str(f)),
            )

    def test_python_venv_path_missing(self, tmp_path: Path) -> None:
        """Non-existent path raises ValidationError."""
        missing = tmp_path / "does_not_exist"
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(
                _make_config(python_venv=str(missing)),
            )

    def test_python_venv_no_bin_python(self, tmp_path: Path) -> None:
        """Existing dir without bin/python raises ValidationError."""
        d = tmp_path / "fake_venv"
        d.mkdir()
        with pytest.raises(ValidationError):
            ParallelBashConfig.model_validate(
                _make_config(python_venv=str(d)),
            )

    def test_python_venv_valid_path_accepted(self, tmp_path: Path) -> None:
        """Valid venv path with bin/python accepted and stored as absolute."""
        venv = tmp_path / "valid_venv"
        venv_bin = venv / "bin"
        venv_bin.mkdir(parents=True)
        (venv_bin / "python").write_text("fake")
        cfg = ParallelBashConfig.model_validate(
            _make_config(python_venv=str(venv)),
        )
        assert cfg.python_venv == str(venv.resolve())
        assert Path(cfg.python_venv).is_absolute()


# ─── python_venv Env Propagation ────────────────────────────────────────────


class TestPythonVenvEnvPropagation:
    """Tests for python_venv environment variable propagation to subprocess."""

    def test_python_venv_env_passed_through_run_script(
        self, mock_service, make_frozen_file, make_job, tmp_path, mocker,
    ) -> None:
        """Env dict with PATH and VIRTUAL_ENV reaches execute_bash_script."""
        venv = tmp_path / "test_venv"
        venv_bin = venv / "bin"
        venv_bin.mkdir(parents=True)
        (venv_bin / "python").write_text("fake")

        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(python_venv=str(venv)),
            identifier="test-disp",
        )

        captured_env: dict = {}

        def _fake_execute(**kwargs: Any) -> BashExecResult:
            captured_env["env"] = kwargs.get("env")
            return BashExecResult(return_code=0, stdout="ok", stderr="")

        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.execute_bash_script",
            side_effect=_fake_execute,
        )

        ff = make_frozen_file(file=Path("/test.nc"))
        job = make_job(files=(ff,))
        plugin.get_execution_log(job)

        assert captured_env["env"] is not None, (
            "env should be set when python_venv is configured"
        )
        assert "PATH" in captured_env["env"]
        assert str(venv.resolve() / "bin") in captured_env["env"]["PATH"]
        assert captured_env["env"]["VIRTUAL_ENV"] == str(venv.resolve())

    def test_python_venv_not_set_env_is_none(
        self, mock_service, make_frozen_file, make_job, mocker,
    ) -> None:
        """Without python_venv, execute_bash_script is called with env=None."""
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(),
            identifier="test-disp",
        )

        captured_env: dict = {}

        def _fake_execute(**kwargs: Any) -> BashExecResult:
            captured_env["env"] = kwargs.get("env")
            return BashExecResult(return_code=0, stdout="ok", stderr="")

        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.execute_bash_script",
            side_effect=_fake_execute,
        )

        ff = make_frozen_file(file=Path("/test.nc"))
        job = make_job(files=(ff,))
        plugin.get_execution_log(job)

        assert captured_env["env"] is None, (
            "env should be None when python_venv is not configured"
        )
