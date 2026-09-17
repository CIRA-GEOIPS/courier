"""Tests for ``courier queues`` CLI sub-app."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kombu.exceptions import ChannelError
from typer.testing import CliRunner

from courier.cli.queues import queues_app

_YAML = """\
apiVersion: runcourier.dev/v1alpha1
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
    result = runner.invoke(queues_app, ["list", str(config_file)])
    assert result.exit_code == 0, result.output
    assert "namespace: ns" in result.output
    assert "ns-JobReady-runner-a" in result.output
    assert "ns-JobReady-runner-b" in result.output


def test_list_namespace_override(runner: CliRunner, config_file: Path) -> None:
    """``--namespace`` overrides the metadata namespace."""
    result = runner.invoke(
        queues_app,
        ["list", str(config_file), "--namespace", "other"],
    )
    assert result.exit_code == 0, result.output
    assert "other-JobReady-runner-a" in result.output
    assert "ns-JobReady-runner-a" not in result.output


def test_prune_requires_candidates(runner: CliRunner, config_file: Path) -> None:
    """``prune`` with no candidates exits non-zero with a diagnostic."""
    result = runner.invoke(queues_app, ["prune", str(config_file)])
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
                str(config_file),
                "--candidate",
                "ns-JobReady-ghost",
                "--candidate",
                "ns-JobReady-runner-a",  # preserved
                "--apply",
            ],
        )
    assert result.exit_code == 0, result.output
    # if_empty=True by default: an orphan holding queued jobs is reported
    # rather than silently discarded.
    channel.queue_delete.assert_called_once_with("ns-JobReady-ghost", if_empty=True)
    assert "deleted:  ns-JobReady-ghost" in result.output


def test_prune_force_deletes_non_empty_queues(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """``--force`` opts out of the if_empty guard for a deliberate purge."""
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
                str(config_file),
                "--candidate",
                "ns-JobReady-ghost",
                "--apply",
                "--force",
            ],
        )
    assert result.exit_code == 0, result.output
    channel.queue_delete.assert_called_once_with("ns-JobReady-ghost", if_empty=False)


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
                str(config_file),
                "--from-file",
                str(listing),
            ],
        )
    assert result.exit_code == 0, result.output
    assert "orphan:   ns-JobReady-ghost" in result.output
    assert "preserve: ns-JobReady-runner-a" in result.output


# Durable per-builder file-found queues (issue #44).


def test_list_includes_a_file_found_queue_per_builder(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """Each job builder's durable queue is part of the expected set."""
    result = runner.invoke(queues_app, ["list", str(config_file)])

    assert result.exit_code == 0
    assert "ns-FilesFound-builder" in result.output
    # The fanout exchange is not a queue and prune deletes queues.
    assert "ns-FilesFoundExchange" not in result.output


def test_list_namespace_override_applies_to_file_found_queues(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """The namespace override reaches the new family too."""
    result = runner.invoke(
        queues_app,
        ["list", str(config_file), "--namespace", "other"],
    )

    assert result.exit_code == 0
    assert "other-FilesFound-builder" in result.output
    assert "ns-FilesFound-builder" not in result.output


def test_prune_preserves_builder_queues_and_flags_legacy_names(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """A live builder queue is preserved; the pre-durable name is an orphan.

    Before the queue was made durable, builders consumed
    ``<ns>-FilesFoundExchange-fanout-<uuid>`` queues that the broker deleted on
    disconnect, so one still present is abandoned. The durable name holds the
    backlog for a builder that is down, and deleting it loses those messages.
    """
    result = runner.invoke(
        queues_app,
        [
            "prune",
            str(config_file),
            "--candidate",
            "ns-FilesFound-builder,ns-FilesFound-renamed,"
            "ns-FilesFoundExchange-fanout-0123456789ab",
        ],
    )

    assert result.exit_code == 0
    assert "preserve: ns-FilesFound-builder" in result.output
    assert "orphan:   ns-FilesFound-renamed" in result.output
    assert "orphan:   ns-FilesFoundExchange-fanout-0123456789ab" in result.output


def test_prune_never_deletes_a_live_builder_queue(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """``--apply`` leaves the expected builder queue alone."""
    with patch("courier.cli.queues.Connection") as conn_cls:
        conn = MagicMock()
        channel = MagicMock()
        conn_cls.return_value.__enter__.return_value = conn
        conn.channel.return_value.__enter__.return_value = channel

        result = runner.invoke(
            queues_app,
            [
                "prune",
                str(config_file),
                "--candidate",
                "ns-FilesFound-builder,ns-FilesFound-gone",
                "--apply",
            ],
        )

    assert result.exit_code == 0
    deleted = [call.args[0] for call in channel.queue_delete.call_args_list]
    assert deleted == ["ns-FilesFound-gone"]


def test_prune_reports_a_non_empty_queue_instead_of_crashing(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """A 406 is reported with the ``--force`` hint.

    RabbitMQ answers an ``if_empty`` delete of a non-empty queue with a
    precondition failure, raised as a ``ChannelError``. The prune loop used to
    catch only ``OperationalError``, so the hint never ran and the command
    aborted with a traceback.
    """
    failure = ChannelError("Queue.delete: (406) PRECONDITION_FAILED - not empty")
    failure.reply_code = 406

    with patch("courier.cli.queues.Connection") as conn_cls:
        conn = MagicMock()
        channel = MagicMock()
        channel.queue_delete.side_effect = failure
        conn_cls.return_value.__enter__.return_value = conn
        conn.channel.return_value.__enter__.return_value = channel

        result = runner.invoke(
            queues_app,
            ["prune", str(config_file), "--candidate", "ns-JobReady-ghost", "--apply"],
        )

    assert result.exit_code == 1
    assert "failed:   ns-JobReady-ghost" in result.output
    assert "(non-empty? rerun with --force)" in result.output


def test_prune_does_not_suggest_force_for_a_missing_queue(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """A 404 gets no ``--force`` hint, because forcing cannot fix it."""
    failure = ChannelError("Queue.delete: (404) NOT_FOUND")
    failure.reply_code = 404

    with patch("courier.cli.queues.Connection") as conn_cls:
        conn = MagicMock()
        channel = MagicMock()
        channel.queue_delete.side_effect = failure
        conn_cls.return_value.__enter__.return_value = conn
        conn.channel.return_value.__enter__.return_value = channel

        result = runner.invoke(
            queues_app,
            ["prune", str(config_file), "--candidate", "ns-JobReady-ghost", "--apply"],
        )

    assert result.exit_code == 1
    assert "(non-empty? rerun with --force)" not in result.output


def test_prune_preserves_dead_letter_queues(
    runner: CliRunner,
    config_file: Path,
) -> None:
    """A prune preserves dead-letter queues.

    ``<queue>-DeadLetter`` names appear in no config; they are derived from the
    queues that do appear. Deleting one discards the only copy of the messages
    the service gave up on.
    """
    result = runner.invoke(
        queues_app,
        [
            "prune",
            str(config_file),
            "--candidate",
            "ns-FilesFound-builder-DeadLetter,ns-JobReady-runner-a-DeadLetter,"
            "ns-DispatcherQueue-DeadLetter,ns-FilesFound-ghost-DeadLetter",
        ],
    )

    assert result.exit_code == 0
    assert "preserve: ns-FilesFound-builder-DeadLetter" in result.output
    assert "preserve: ns-JobReady-runner-a-DeadLetter" in result.output
    assert "preserve: ns-DispatcherQueue-DeadLetter" in result.output
    # Derived from a queue no config names: still an orphan.
    assert "orphan:   ns-FilesFound-ghost-DeadLetter" in result.output
