"""Behavioural tests for the ``JobBuilder`` base class.

This machinery — the consume loop, ``emit()`` fan-out, and the claim/pop
lifecycle — had no dedicated test file, and three of the critical bugs lived
here: jobs dropped when a route declared no targets, ready jobs claimed too
late to be exclusive, and a consume loop that never observed shutdown.

Tests exercise the real methods against a stub service and assert on observable
outcomes (what got published, what the group contains, what the gauge reads)
rather than on internal shape.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import FatalBrokerError, TransientBrokerError
from courier.interfaces.job_builders import JobBuilder
from courier.types.file import File, FrozenFile
from courier.types.job import Job, JobGroup


class _CountingJob(Job):
    """Ready once it holds ``capacity`` files; rejects beyond that."""

    capacity = 2

    def ready(self) -> bool:
        return len(self.files) >= self.capacity

    def add_file(self, file: File | FrozenFile) -> bool:
        if len(self.files) >= self.capacity:
            return False
        return super().add_file(file)


class _StubGroup(JobGroup):
    """Every file is relevant and maps to one fixed bucket."""

    def __init__(self, name: str = "grp") -> None:
        super().__init__(name, {})
        self.job = _CountingJob

    def file_is_relevant(self, file: File | FrozenFile) -> bool:
        return True

    def get_job_ids_from_file(self, file: File | FrozenFile) -> list[str]:
        return ["bucket"]


def _builder(service: MagicMock, identifier: str = "jb-1") -> JobBuilder:
    builder = JobBuilder(service, {"targets": ["dp-1"]}, identifier=identifier)
    builder.job_groups = [_StubGroup()]
    return builder


def _file(name: str) -> FrozenFile:
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h")


@pytest.fixture
def service() -> MagicMock:
    """Service stub whose ``emit`` records every publish."""
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc.target_resolver.resolve.side_effect = lambda ident: f"JobReady-{ident}"
    return svc


# ── emit() fan-out ──────────────────────────────────────────────────────────


class TestEmit:
    """``emit`` is the only path jobs take out of a builder."""

    def test_publishes_once_per_target(self, service: MagicMock) -> None:
        builder = _builder(service)
        builder.emit(Job("n", "job-1", {}), ["dp-a", "dp-b"])

        queues = [call.kwargs["queue"] for call in service.emit.call_args_list]
        assert queues == ["JobReady-dp-a", "JobReady-dp-b"]

    def test_falls_back_to_configured_targets(self, service: MagicMock) -> None:
        """``targets=None`` uses the builder's own list, not nothing."""
        builder = _builder(service)
        builder.emit(Job("n", "job-1", {}))

        assert service.emit.call_count == 1
        assert service.emit.call_args.kwargs["queue"] == "JobReady-dp-1"

    def test_no_targets_drops_the_job_and_logs_an_error(
        self,
        service: MagicMock,
    ) -> None:
        """Silently dropping is the failure mode; make it loud and visible."""
        builder = _builder(service)
        builder._logger = MagicMock()

        builder.emit(Job("n", "job-1", {}), [])

        service.emit.assert_not_called()
        builder._logger.error.assert_called_once()
        assert "dropping" in builder._logger.error.call_args[0][0]

    def test_stamps_emit_time_and_targets_on_the_published_job(
        self,
        service: MagicMock,
    ) -> None:
        """The dispatcher reads emit_time to compute routing latency."""
        builder = _builder(service)
        job = Job("n", "job-1", {})
        before = time.time()

        builder.emit(job, ["dp-a", "dp-b"])

        assert job.emit_time is not None
        assert job.emit_time >= before
        assert job.targets == ("dp-a", "dp-b")

        published = Job.from_string(service.emit.call_args.kwargs["message"])
        assert published.targets == ("dp-a", "dp-b")
        assert published.emit_time == job.emit_time

    def test_correlation_id_survives_to_the_dispatcher(
        self,
        service: MagicMock,
    ) -> None:
        """Correlation IDs are the only cross-stage log join key."""
        builder = _builder(service)
        job = Job("n", "job-1", {}, correlation_id="corr-abc")

        builder.emit(job, ["dp-a"])

        published = Job.from_string(service.emit.call_args.kwargs["message"])
        assert published.correlation_id == "corr-abc"

    def test_partial_fanout_still_delivers_to_healthy_targets(
        self,
        service: MagicMock,
    ) -> None:
        """One broken target must not cost the others their job."""
        builder = _builder(service)
        builder._logger = MagicMock()
        service.emit.side_effect = [FatalBrokerError("boom"), None]

        builder.emit(Job("n", "job-1", {}), ["dp-bad", "dp-good"])

        assert service.emit.call_count == 2
        message = builder._logger.error.call_args[0][0]
        assert "partial fan-out" in message
        assert "dp-good" in message

    def test_transient_failure_is_retried(self, service: MagicMock) -> None:
        """Transient broker errors retry; a blip should not lose a job."""
        builder = _builder(service)
        service.emit.side_effect = [TransientBrokerError("blip"), None]

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        assert service.emit.call_count == 2

    def test_publishes_with_confirm(self, service: MagicMock) -> None:
        """Jobs are published with publisher confirms, not fire-and-forget."""
        builder = _builder(service)
        builder.emit(Job("n", "job-1", {}), ["dp-a"])
        assert service.emit.call_args.kwargs["confirm"] is True


# ── claim / pop lifecycle ───────────────────────────────────────────────────


class TestReadyJobLifecycle:
    """A ready job must be emitted exactly once and then leave the group."""

    def test_ready_job_is_emitted_and_removed(self, service: MagicMock) -> None:
        builder = _builder(service)
        group = builder.job_groups[0]

        builder._process_job_group(group, _file("a"))
        assert service.emit.call_count == 0, "not ready after one file"

        builder._process_job_group(group, _file("b"))
        assert service.emit.call_count == 1
        assert group.jobs == {}, "emitted job must not linger in the group"

    def test_emitted_job_carries_all_its_files(self, service: MagicMock) -> None:
        builder = _builder(service)
        group = builder.job_groups[0]
        builder._process_job_group(group, _file("a"))
        builder._process_job_group(group, _file("b"))

        published = Job.from_string(service.emit.call_args.kwargs["message"])
        assert {str(f.file) for f in published.files} == {
            "/data/a.nc",
            "/data/b.nc",
        }

    def test_a_second_batch_gets_a_fresh_identifier(
        self,
        service: MagicMock,
    ) -> None:
        """Reusing an identifier makes the dispatcher's LRU drop the job."""
        builder = _builder(service)
        group = builder.job_groups[0]
        for name in ("a", "b", "c", "d"):
            builder._process_job_group(group, _file(name))

        identifiers = [
            Job.from_string(call.kwargs["message"]).identifier
            for call in service.emit.call_args_list
        ]
        assert len(identifiers) == 2
        assert len(set(identifiers)) == 2, f"identifier reused: {identifiers}"

    def test_ready_jobs_are_claimed_under_the_group_lock(
        self,
        service: MagicMock,
    ) -> None:
        """A concurrent reaper must never see a job that is already in flight.

        Regression guard for the double-emit window: ready jobs used to be
        listed under the lock but removed only after ``emit()`` returned.
        """
        builder = _builder(service)
        group = builder.job_groups[0]
        builder._group_locks = {group.name: threading.Lock()}
        observed: list[int] = []

        def _observe_during_emit(**_kwargs: Any) -> None:
            # Emission happens outside the lock; by then the group must
            # already be empty.
            observed.append(len(group.jobs))

        service.emit.side_effect = _observe_during_emit

        builder._process_job_group(group, _file("a"))
        builder._process_job_group(group, _file("b"))

        assert observed == [0], (
            f"job still in group while being emitted: {observed}"
        )

    def test_files_beyond_capacity_are_not_dropped(
        self,
        service: MagicMock,
    ) -> None:
        """A full job must overflow into a successor, never discard."""
        builder = _builder(service)
        group = builder.job_groups[0]
        for name in ("a", "b", "c"):
            builder._process_job_group(group, _file(name))

        emitted = {
            str(f.file)
            for call in service.emit.call_args_list
            for f in Job.from_string(call.kwargs["message"]).files
        }
        still_open = {str(f.file) for j in group.jobs.values() for f in j.files}
        assert emitted | still_open == {"/data/a.nc", "/data/b.nc", "/data/c.nc"}

    def test_timed_out_jobs_are_discarded(self, service: MagicMock) -> None:
        builder = _builder(service)
        group = builder.job_groups[0]
        stale = _CountingJob("n", "stale", {}, last_modified=0.0, timeout=1.0)
        group.jobs["stale"] = stale

        builder._cleanup_old_jobs(group)

        assert "stale" not in group.jobs


# ── metrics ─────────────────────────────────────────────────────────────────


class TestEmittedMetrics:
    """Metrics are read from the registry, not from the metric object."""

    def test_successful_emit_increments_per_target_counter(
        self,
        service: MagicMock,
    ) -> None:
        builder = _builder(service, identifier="jb-metrics")
        labels = {
            "job_builder_name": builder.name,
            "job_builder_identifier": "jb-metrics",
            "target": "dp-a",
        }
        before = REGISTRY.get_sample_value(
            "courier_job_builder_jobs_emitted_total", labels,
        ) or 0.0

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        after = REGISTRY.get_sample_value(
            "courier_job_builder_jobs_emitted_total", labels,
        )
        assert after == before + 1

    def test_failed_emit_increments_failure_counter_with_reason(
        self,
        service: MagicMock,
    ) -> None:
        builder = _builder(service, identifier="jb-fail")
        builder._logger = MagicMock()
        service.emit.side_effect = FatalBrokerError("nope")
        labels = {
            "job_builder_name": builder.name,
            "job_builder_identifier": "jb-fail",
            "target": "dp-a",
            "reason": "fatal",
        }
        before = REGISTRY.get_sample_value(
            "courier_job_builder_emit_failures_total", labels,
        ) or 0.0

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        after = REGISTRY.get_sample_value(
            "courier_job_builder_emit_failures_total", labels,
        )
        assert after == before + 1


# ── lifecycle ───────────────────────────────────────────────────────────────


class TestLifecycle:
    """Start/stop must actually start and stop, and be re-entrant."""

    def test_stop_signals_the_consume_loop(self, service: MagicMock) -> None:
        """Without this the non-daemon consumer wedged interpreter shutdown."""
        builder = _builder(service)
        service.consume.return_value = iter(())

        builder.start()
        assert builder.is_healthy() is True
        builder.stop()

        assert builder._stop_event.is_set()
        assert builder._state is PluginRunState.STOPPED
        assert not (builder._main_thread and builder._main_thread.is_alive())

    def test_consume_receives_the_stop_event_and_exchange(
        self,
        service: MagicMock,
    ) -> None:
        """The stop event must reach the broker loop, not just be stored."""
        builder = _builder(service)
        service.consume.return_value = iter(())

        builder.handle_incoming_files()

        assert service.consume.call_args[0][0] == FILE_FOUND_EXCHANGE
        assert service.consume.call_args.kwargs["stop_event"] is builder._stop_event

    def test_start_is_idempotent(self, service: MagicMock) -> None:
        builder = _builder(service)
        service.consume.return_value = iter(())
        builder.start()
        first_thread = builder._main_thread

        builder.start()

        assert builder._main_thread is first_thread
        builder.stop()

    def test_incoming_file_is_routed_to_every_group(
        self,
        service: MagicMock,
    ) -> None:
        """Each group independently decides relevance; none may be skipped."""
        builder = _builder(service)
        builder.job_groups = [_StubGroup("g1"), _StubGroup("g2")]
        service.consume.return_value = iter([(str(File(file=Path("/d/x.nc"))), None)])

        builder.handle_incoming_files()

        assert all(g.jobs for g in builder.job_groups)
