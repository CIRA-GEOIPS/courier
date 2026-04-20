"""Tests for ``courier queues`` CLI sub-app."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from courier.cli.queues import queues_app

_YAML = """\
apiVersion: courier.dev/v1alpha1
kind: Service
metadata:
  name: test-svc
  namespace: ns
  description: test
spec:
  broker:
    transport: memory
  run:
    - watcher:
        kind: data_monitors
        name: file_system_poller_watchdog
        config: {}
    - builder:
        kind: job_builders
        name: filter_pass
        config: {files_per_job: 1, targets: [runner-a]}
    - runner-a:
        kind: dispatchers
        name: serial_bash
        config: {bash_script: "echo a"}
    - runner-b:
        kind: dispatchers
        name: serial_bash
        config: {bash_script: "echo b"}
"""


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    path = tmp_path / "svc.yaml"
    path.write_text(_YAML)
    return path


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_list_prints_expected_queues(runner: CliRunner, config_file: Path) -> None:
    """``list`` prints namespace-prefixed expected queues for every dispatcher."""
    result = runner.invoke(queues_app, ["list", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    assert "namespace: ns" in result.output
    assert "ns-JobReady-runner-a" in result.output
    assert "ns-JobReady-runner-b" in result.output


def test_list_namespace_override(runner: CliRunner, config_file: Path) -> None:
    """``--namespace`` overrides the metadata namespace."""
    result = runner.invoke(
        queues_app,
        ["list", "--config", str(config_file), "--namespace", "other"],
    )
    assert result.exit_code == 0, result.output
    assert "other-JobReady-runner-a" in result.output
    assert "ns-JobReady-runner-a" not in result.output


def test_prune_requires_candidates(runner: CliRunner, config_file: Path) -> None:
    """``prune`` with no candidates exits non-zero with a diagnostic."""
    result = runner.invoke(queues_app, ["prune", "--config", str(config_file)])
    assert result.exit_code == 2
    assert "no candidates" in result.output


def test_prune_dry_run_reports_orphans(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """Dry run reports orphans + preserves without calling the broker."""
    with patch("courier.cli.queues.Connection") as conn_cls:
        result = runner.invoke(
            queues_app,
            [
                "prune",
                "--config",
                str(config_file),
                "--candidate",
                "ns-JobReady-runner-a,ns-JobReady-ghost,ns-other-orphan",
            ],
        )
    assert result.exit_code == 0, result.output
    assert "preserve: ns-JobReady-runner-a" in result.output
    assert "orphan:   ns-JobReady-ghost" in result.output
    assert "orphan:   ns-other-orphan" in result.output
    assert "dry-run" in result.output
    conn_cls.assert_not_called()


def test_prune_no_orphans_short_circuits(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """All-expected candidates produce a clean no-op exit."""
    with patch("courier.cli.queues.Connection") as conn_cls:
        result = runner.invoke(
            queues_app,
            [
                "prune",
                "--config",
                str(config_file),
                "--candidate",
                "ns-JobReady-runner-a",
                "--candidate",
                "ns-JobReady-runner-b",
                "--apply",
            ],
        )
    assert result.exit_code == 0, result.output
    assert "no orphans" in result.output
    conn_cls.assert_not_called()


def test_prune_apply_deletes_orphans(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """``--apply`` opens a connection and calls ``queue_delete`` per orphan."""
    channel = MagicMock()
    conn = MagicMock()
    conn.channel.return_value.__enter__.return_value = channel
    conn_cls = MagicMock()
    conn_cls.return_value.__enter__.return_value = conn

    with patch("courier.cli.queues.Connection", conn_cls):
        result = runner.invoke(
            queues_app,
            [
                "prune",
                "--config",
                str(config_file),
                "--candidate",
                "ns-JobReady-ghost",
                "--candidate",
                "ns-JobReady-runner-a",  # preserved
                "--apply",
            ],
        )
    assert result.exit_code == 0, result.output
    channel.queue_delete.assert_called_once_with("ns-JobReady-ghost")
    assert "deleted:  ns-JobReady-ghost" in result.output


def test_prune_from_file(
    runner: CliRunner,
    config_file: Path,
    tmp_path: Path,
) -> None:
    """``--from-file`` strips comments/blank lines and feeds candidates in."""
    listing = tmp_path / "queues.txt"
    listing.write_text(
        "# orphans to check\nns-JobReady-ghost\n\n  ns-JobReady-runner-a  \n",
    )
    with patch("courier.cli.queues.Connection"):
        result = runner.invoke(
            queues_app,
            [
                "prune",
                "--config",
                str(config_file),
                "--from-file",
                str(listing),
            ],
        )
    assert result.exit_code == 0, result.output
    assert "orphan:   ns-JobReady-ghost" in result.output
    assert "preserve: ns-JobReady-runner-a" in result.output
