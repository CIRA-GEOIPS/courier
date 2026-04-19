"""Unit tests for the slurm_dispatcher plugin."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.errors import PluginStartupError
from courier.plugins.classes.dispatchers.slurm_dispatcher import (
    SlurmDispatcher,
    SlurmDispatcherConfig,
)
from courier.types.execution_log import ExecutionLog


def _make_config(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "sbatch_template": "#!/bin/bash\n#SBATCH --job-name={{ job.name }}\necho ok",
        "slurm_output_dir": str(tmp_path),
    }
    cfg.update(overrides)
    return cfg


# ─── Config ─────────────────────────────────────────────────────────────────


class TestSlurmDispatcherConfig:
    def test_minimal_valid(self, tmp_path: Path) -> None:
        cfg = SlurmDispatcherConfig.model_validate(_make_config(tmp_path))
        assert cfg.wait_for_completion is True

    def test_invalid_template_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError, match="Invalid sbatch_template"):
            SlurmDispatcherConfig.model_validate(
                _make_config(tmp_path, sbatch_template="{{ unclosed"),
            )


# ─── Constructor / start ────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes(self, mock_service: MagicMock, tmp_path: Path) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        assert plugin._output_dir == tmp_path

    def test_start_requires_sbatch(
        self, mock_service: MagicMock, tmp_path: Path, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        mocker.patch(
            "courier.plugins.classes.dispatchers.slurm_dispatcher.shutil.which",
            return_value=None,
        )
        with pytest.raises(PluginStartupError, match="sbatch"):
            plugin.start()


# ─── _parse_sacct_output ────────────────────────────────────────────────────


class TestParseSacctOutput:
    def test_completed(self, mock_service: MagicMock, tmp_path: Path) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        state, rc = plugin._parse_sacct_output("COMPLETED|0:0\n")
        assert state == "COMPLETED"
        assert rc == 0

    def test_failed_with_exit_code(
        self, mock_service: MagicMock, tmp_path: Path
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        state, rc = plugin._parse_sacct_output("FAILED|2:0")
        assert state == "FAILED"
        assert rc == 2

    def test_empty_returns_blank(
        self, mock_service: MagicMock, tmp_path: Path
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        state, rc = plugin._parse_sacct_output("")
        assert state == ""
        assert rc == 0


# ─── _submit ────────────────────────────────────────────────────────────────


class TestSubmit:
    def test_parses_job_id_from_stdout_verbose(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        result = MagicMock(returncode=0, stdout="Submitted batch job 12345\n", stderr="")
        mocker.patch(
            "courier.plugins.classes.dispatchers.slurm_dispatcher.subprocess.run",
            return_value=result,
        )
        job_id = plugin._submit(make_job(), tmp_path / "s.sbatch")
        assert job_id == "12345"

    def test_parses_parsable_output(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        result = MagicMock(returncode=0, stdout="98765\n", stderr="")
        mocker.patch(
            "courier.plugins.classes.dispatchers.slurm_dispatcher.subprocess.run",
            return_value=result,
        )
        assert plugin._submit(make_job(), tmp_path / "s.sbatch") == "98765"

    def test_nonzero_returncode_yields_none(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        result = MagicMock(returncode=1, stdout="", stderr="rejected")
        mocker.patch(
            "courier.plugins.classes.dispatchers.slurm_dispatcher.subprocess.run",
            return_value=result,
        )
        assert plugin._submit(make_job(), tmp_path / "s.sbatch") is None
        assert plugin._last_submit_error == "rejected"


# ─── get_execution_log ──────────────────────────────────────────────────────


class TestGetExecutionLog:
    def test_no_wait_returns_submit_log(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(
            mock_service, _make_config(tmp_path, wait_for_completion=False),
        )
        mocker.patch.object(plugin, "_submit", return_value="42")
        logs = plugin.get_execution_log(make_job())
        assert len(logs) == 1
        assert isinstance(logs[0], ExecutionLog)
        assert logs[0].return_code == 0
        assert "42" in logs[0].stdout

    def test_full_cycle_completed(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))
        mocker.patch.object(plugin, "_submit", return_value="77")
        mocker.patch.object(plugin, "_poll_status", return_value=("COMPLETED", 0))
        mocker.patch.object(plugin, "_read_output", return_value=("out", ""))
        logs = plugin.get_execution_log(make_job())
        assert logs[0].return_code == 0
        assert logs[0].stdout == "out"

    def test_submission_failure_returns_error_log(
        self, mock_service: MagicMock, tmp_path: Path, make_job, mocker
    ) -> None:
        plugin = SlurmDispatcher(mock_service, _make_config(tmp_path))

        def fake_submit(*_args, **_kwargs):
            plugin._last_submit_error = "oops"
            return None

        mocker.patch.object(plugin, "_submit", side_effect=fake_submit)
        logs = plugin.get_execution_log(make_job())
        assert logs[0].return_code == -1
        assert logs[0].stderr == "oops"
