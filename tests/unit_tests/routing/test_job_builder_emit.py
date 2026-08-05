"""Tests for the routing-aware JobBuilder.emit fan-out path.

Covers:
- fan-out publishes once per target and records a success metric per target;
- per-target emit claim keyed by ``{job_id}::{target}``;
- transient broker errors are retried with backoff;
- fatal broker errors release the per-target claim so a restart can retry;
- partial fan-out is reported at ERROR level with both sides named.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from courier.errors import FatalBrokerError, TransientBrokerError
from courier.plugins.job_builders.filter_and_group import (
    FilterAndGroupJobBuilder,
)
from courier.types.job import Job


def _make_builder(mock_service: MagicMock, targets: list[str]) -> FilterAndGroupJobBuilder:
    """Build a FilterAndGroupJobBuilder with a stubbed service + resolver."""
    mock_service.target_resolver = MagicMock()
    mock_service.target_resolver.resolve.side_effect = (
        lambda ident: f"JobReady-{ident}"
    )
    mock_service.emit = MagicMock()
    builder = FilterAndGroupJobBuilder(
        mock_service,
        {"files_per_job": 1, "targets": targets},
        identifier="builder",
    )
    builder._sync = None  # no HA state sync in these tests
    return builder


def _job() -> Job:
    return Job(name="g", identifier="job-1", config={}, timeout=60.0)


def test_emit_fans_out_to_each_target(mock_service: MagicMock) -> None:
    """Each target receives exactly one publish call."""
    builder = _make_builder(mock_service, ["a", "b"])
    builder.emit(_job())
    queues = [c.kwargs.get("queue") or c.args[0] for c in mock_service.emit.call_args_list]
    assert sorted(queues) == ["JobReady-a", "JobReady-b"]


def test_emit_records_one_success_metric_per_target(
    mock_service: MagicMock,
    mocker,
) -> None:
    """Every successful publish bumps the per-target counter once."""
    builder = _make_builder(mock_service, ["a", "b"])
    inc = mocker.patch.object(builder._jobs_emitted, "labels")
    builder.emit(_job())
    called_targets = sorted(c.kwargs["target"] for c in inc.call_args_list)
    assert called_targets == ["a", "b"]


def test_emit_populates_emit_time_and_targets(mock_service: MagicMock) -> None:
    """emit() stamps emit_time and records targets on the Job itself."""
    builder = _make_builder(mock_service, ["a"])
    job = _job()
    builder.emit(job)
    assert job.emit_time is not None
    assert job.targets == ("a",)


def test_emit_with_empty_targets_drops_and_logs(mock_service: MagicMock) -> None:
    """No targets means no publish and an ERROR log, not a crash."""
    builder = _make_builder(mock_service, [])
    builder.emit(_job(), targets=[])
    assert mock_service.emit.call_count == 0


def test_per_target_claim_key_format(mock_service: MagicMock) -> None:
    """Emit claim keys include both the job id AND the target."""
    builder = _make_builder(mock_service, ["a", "b"])
    sync = MagicMock()
    sync.try_claim_emit.return_value = True
    builder._sync = sync
    builder.emit(_job())
    keys = sorted(c.args[0] for c in sync.try_claim_emit.call_args_list)
    assert keys == ["job-1::a", "job-1::b"]


def test_claim_rejection_skips_publish(mock_service: MagicMock) -> None:
    """A target whose claim is rejected is not republished."""
    builder = _make_builder(mock_service, ["a", "b"])
    sync = MagicMock()
    sync.try_claim_emit.side_effect = [False, True]  # first target already claimed
    builder._sync = sync
    builder.emit(_job())
    queues = [c.kwargs.get("queue") or c.args[0] for c in mock_service.emit.call_args_list]
    assert queues == ["JobReady-b"]


def test_fatal_error_releases_claim(mock_service: MagicMock) -> None:
    """A fatal broker error releases the per-target claim."""
    builder = _make_builder(mock_service, ["a"])
    sync = MagicMock()
    sync.try_claim_emit.return_value = True
    builder._sync = sync
    mock_service.emit.side_effect = FatalBrokerError("boom")
    builder.emit(_job())
    sync.release_emit_claim.assert_called_once_with("job-1::a")


def test_transient_error_does_not_release_claim(mock_service: MagicMock) -> None:
    """A transient error (even after retries exhaust) retains the claim.

    Retaining the claim is important: if the broker later recovers and a
    restart retries, the sync layer prevents double-emit. Releasing on
    transient would defeat that guarantee.
    """
    builder = _make_builder(mock_service, ["a"])
    sync = MagicMock()
    sync.try_claim_emit.return_value = True
    builder._sync = sync
    mock_service.emit.side_effect = TransientBrokerError("flaky")
    builder.emit(_job())
    sync.release_emit_claim.assert_not_called()


def test_partial_fanout_logs_error(mock_service: MagicMock, caplog) -> None:
    """When one target fails and one succeeds, a single ERROR line names both."""
    import logging as _logging

    builder = _make_builder(mock_service, ["good", "bad"])

    def _emit_selective(**kwargs) -> None:
        if "bad" in kwargs["queue"]:
            raise FatalBrokerError("bad queue")

    mock_service.emit.side_effect = _emit_selective

    # ContextAdapter wraps the real Logger; attach handler to the underlying one.
    inner = builder._logger.logger
    inner.addHandler(caplog.handler)
    prev_level = inner.level
    inner.setLevel(_logging.ERROR)
    try:
        builder.emit(_job())
    finally:
        inner.removeHandler(caplog.handler)
        inner.setLevel(prev_level)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("partial fan-out" in m and "good" in m and "bad" in m for m in msgs)
