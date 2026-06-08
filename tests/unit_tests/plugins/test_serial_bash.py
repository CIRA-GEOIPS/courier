"""Unit tests for the serial_bash dispatcher plugin (Jinja2-based)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pydantic
import pytest

from courier.plugins.classes.dispatchers.serial_bash import (
    SerialBashConfig,
    SerialBashDispatcher,
)
from courier.types.execution_log import ExecutionLog


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

    def test_module_init_short_circuits(self) -> None:
        plugin = SerialBashDispatcher(None, None)
        assert not hasattr(plugin, "validated")

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
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        run = mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            return_value=fake_completed_process(returncode=0, stdout="hi"),
        )
        job = make_job(files=(make_frozen_file(file=Path("/tmp/a.nc")),))
        logs = plugin.get_execution_log(job)

        assert len(logs) == 1
        assert isinstance(logs[0], ExecutionLog)
        assert logs[0].return_code == 0
        assert logs[0].stdout == "hi"
        run.assert_called_once()

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
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="bash", timeout=1),
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
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=OSError("boom"),
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
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service, _make_config(), identifier="test-disp",
        )
        captured: list[str] = []

        def fake_run(args, **_kwargs):
            captured.append(args[1])
            return fake_completed_process()

        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=fake_run,
        )
        job = make_job(files=(make_frozen_file(),))
        plugin.get_execution_log(job)
        assert captured
        assert not Path(captured[0]).exists()

    def test_multi_file_rendering(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(
            mock_service,
            _make_config(
                bash_script="{% for f in files %}echo {{ f.file }};{% endfor %}",
            ),
            identifier="test-disp",
        )
        captured_content: list[str] = []

        def fake_run(args, **_kwargs):
            script_path = args[1]
            captured_content.append(Path(script_path).read_text())
            return fake_completed_process()

        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=fake_run,
        )
        job = make_job(
            files=(
                make_frozen_file(file=Path("/tmp/a.nc")),
                make_frozen_file(file=Path("/tmp/b.nc")),
                make_frozen_file(file=Path("/tmp/c.nc")),
            ),
        )
        plugin.get_execution_log(job)

        assert len(captured_content) == 1
        script = captured_content[0]
        assert "/tmp/a.nc" in script
        assert "/tmp/b.nc" in script
        assert "/tmp/c.nc" in script
