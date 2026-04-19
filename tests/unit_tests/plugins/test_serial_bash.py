"""Unit tests for the serial_bash dispatcher plugin."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.plugins.classes.dispatchers.serial_bash import SerialBashDispatcher
from courier.types.execution_log import ExecutionLog


def _make_config(**overrides):
    cfg = {"bash_script": "echo {file}"}
    cfg.update(overrides)
    return cfg


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_stores_config(self, mock_service: MagicMock) -> None:
        plugin = SerialBashDispatcher(mock_service, _make_config())
        assert plugin.bash_script == "echo {file}"

    def test_module_init_short_circuits(self) -> None:
        plugin = SerialBashDispatcher(None, None)
        assert not hasattr(plugin, "bash_script")

    def test_missing_bash_script_raises(self, mock_service: MagicMock) -> None:
        with pytest.raises(KeyError):
            SerialBashDispatcher(mock_service, {})


# ─── is_healthy ─────────────────────────────────────────────────────────────


class TestIsHealthy:
    def test_always_healthy(self, mock_service: MagicMock) -> None:
        plugin = SerialBashDispatcher(mock_service, _make_config())
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
        plugin = SerialBashDispatcher(mock_service, _make_config())
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

    def test_timeout_returns_failure_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(mock_service, _make_config())
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="bash", timeout=1),
        )
        job = make_job(files=(make_frozen_file(),))
        logs = plugin.get_execution_log(job)
        assert logs[0].return_code == -1
        assert "timed out" in logs[0].stderr

    def test_subprocess_error_returns_failure_log(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(mock_service, _make_config())
        mocker.patch(
            "courier.plugins.classes.dispatchers.serial_bash.subprocess.run",
            side_effect=OSError("boom"),
        )
        job = make_job(files=(make_frozen_file(),))
        logs = plugin.get_execution_log(job)
        assert logs[0].return_code == -1
        assert "boom" in logs[0].stderr

    def test_script_file_cleaned_up(
        self,
        mock_service: MagicMock,
        make_frozen_file,
        make_job,
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = SerialBashDispatcher(mock_service, _make_config())
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
