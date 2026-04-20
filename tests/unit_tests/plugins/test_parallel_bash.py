"""Unit tests for the parallel_bash dispatcher plugin."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.plugins.classes.dispatchers.parallel_bash import (
    ParallelBashConfig,
    ParallelBashDispatcher,
    _run_script,
)
from courier.types.execution_log import ExecutionLog


def _make_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"bash_script": "echo {file}"}
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


# ─── Constructor / health ───────────────────────────────────────────────────


class TestConstructor:
    def test_initializes(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.validated.bash_script == "echo {file}"

    def test_always_healthy(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin.is_healthy() is True


# ─── _render_script ─────────────────────────────────────────────────────────


class TestRenderScript:
    def test_substitutes_file(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(), identifier="test-disp")
        assert plugin._render_script("/tmp/x") == "echo /tmp/x"

    def test_missing_key_falls_back(self, mock_service: MagicMock) -> None:
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(bash_script="echo {missing}"),
            identifier="test-disp",
        )
        assert plugin._render_script("/tmp/x") == "echo {missing}"


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
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = ParallelBashDispatcher(mock_service, _make_config(max_workers=2), identifier="test-disp")
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.subprocess.run",
            return_value=fake_completed_process(returncode=0),
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
        fake_completed_process,
        mocker,
    ) -> None:
        plugin = ParallelBashDispatcher(
            mock_service,
            _make_config(max_workers=1, fail_fast=True),
            identifier="test-disp",
        )
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.subprocess.run",
            return_value=fake_completed_process(returncode=2),
        )
        files = tuple(
            make_frozen_file(file=Path(f"/f{i}.nc")) for i in range(5)
        )
        logs = plugin.get_execution_log(make_job(files=files))
        assert len(logs) >= 1
        assert logs[0].return_code == 2


# ─── _run_script (module-level helper) ──────────────────────────────────────


class TestRunScript:
    def test_success(self, fake_completed_process, mocker) -> None:
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.subprocess.run",
            return_value=fake_completed_process(returncode=0, stdout="ok"),
        )
        log = _run_script("echo hi", 60.0, "h1")
        assert log.return_code == 0
        assert log.stdout == "ok"

    def test_oserror_returns_failure(self, mocker) -> None:
        mocker.patch(
            "courier.plugins.classes.dispatchers.parallel_bash.subprocess.run",
            side_effect=OSError("boom"),
        )
        log = _run_script("x", 60.0, "h")
        assert log.return_code == -1
        assert "boom" in log.stderr
