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

import contextlib
import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import FatalBrokerError, TransientBrokerError
from courier.interfaces.job_builders import JobBuilder
from courier.interfaces.payloads import Payload
from courier.types.file import File, FrozenFile
from courier.types.job import Job, JobGroup
from tests._helpers import DEFAULT_PAYLOAD_ID, bind_payload, with_payload

if TYPE_CHECKING:
    from collections.abc import Iterator


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
    builder = JobBuilder(
        service,
        with_payload({"targets": ["dp-1"]}),
        identifier=identifier,
    )
    builder.job_groups = [_StubGroup()]
    bind_payload(builder)
    return builder


def _file(name: str) -> FrozenFile:
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h")


@contextlib.contextmanager
def _captured(
    builder: JobBuilder,
    caplog: pytest.LogCaptureFixture,
    level: int = logging.ERROR,
) -> Iterator[None]:
    """Route *builder*'s log records into *caplog* while the block runs.

    ``get_logger`` makes every courier logger non-propagating, and pytest
    attaches caplog's handler only to non-propagating loggers that already
    exist when a test phase starts. A builder created inside the test would
    otherwise log straight past caplog whenever it was the first test in its
    process to build one, so the assertion depended on test order.
    """
    logger = builder._logger.logger
    added = caplog.handler not in logger.handlers
    if added:
        logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(level, logger=logger.name):
            yield
    finally:
        if added:
            logger.removeHandler(caplog.handler)


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

        assert observed == [0], f"job still in group while being emitted: {observed}"

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
        before = (
            REGISTRY.get_sample_value(
                "courier_job_builder_jobs_emitted_total",
                labels,
            )
            or 0.0
        )

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        after = REGISTRY.get_sample_value(
            "courier_job_builder_jobs_emitted_total",
            labels,
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
        before = (
            REGISTRY.get_sample_value(
                "courier_job_builder_emit_failures_total",
                labels,
            )
            or 0.0
        )

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        after = REGISTRY.get_sample_value(
            "courier_job_builder_emit_failures_total",
            labels,
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


class TestPoisonMessages:
    """A body that will not parse is dropped.

    The file-found queue is durable, so a message no consumer can parse is
    redelivered until something drops it.
    """

    def test_malformed_bodies_are_counted_logged_and_skipped(
        self,
        service: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Three unusable bodies are dropped and the good one still lands."""
        builder = _builder(service)
        before = (
            REGISTRY.get_sample_value(
                "courier_job_builder_malformed_messages_total",
                {"job_builder_name": builder.name, "job_builder_identifier": "jb-1"},
            )
            or 0.0
        )

        service.consume.return_value = iter(
            [
                ("not json at all", None),
                # A JSON array parses, then fails on attribute access, which an
                # enumerated (ValueError, KeyError) guard misses.
                ("[]", None),
                ("42", None),
                (str(_file("good")), None),
            ],
        )

        with _captured(builder, caplog):
            builder.handle_incoming_files()

        after = REGISTRY.get_sample_value(
            "courier_job_builder_malformed_messages_total",
            {"job_builder_name": builder.name, "job_builder_identifier": "jb-1"},
        )
        assert after - before == 3
        assert sum(len(g.jobs) for g in builder.job_groups) == 1
        assert (
            sum(
                "Dropping malformed file-found message" in r.message
                for r in caplog.records
            )
            == 3
        )

    def test_the_logged_preview_is_truncated(
        self,
        service: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A huge body does not paste itself into the log in full."""
        builder = _builder(service)
        service.consume.return_value = iter([("x" * 5000, None)])

        with _captured(builder, caplog):
            builder.handle_incoming_files()

        assert any("Dropping malformed" in r.message for r in caplog.records)
        assert all(len(r.getMessage()) < 2000 for r in caplog.records)

    def test_the_subscriber_identifier_is_passed_to_consume(
        self,
        service: MagicMock,
    ) -> None:
        """The builder names the durable queue it consumes from."""
        builder = _builder(service, identifier="jb-7")
        service.consume.return_value = iter(())

        builder.handle_incoming_files()

        assert service.consume.call_args.kwargs["subscriber"] == "jb-7"


# ── pass-one payload render failures ────────────────────────────────────────


class _OneFileJob(_CountingJob):
    """Ready as soon as it holds one file."""

    capacity = 1


class _OneFileGroup(_StubGroup):
    """One job per file, so each file's render stands alone."""

    def __init__(self, name: str = "grp") -> None:
        super().__init__(name)
        self.job = _OneFileJob


def _render_failures(identifier: str, target: str = "dp-1") -> float:
    """Return the render-failure count for one builder and target."""
    return (
        REGISTRY.get_sample_value(
            "courier_job_builder_emit_failures_total",
            {
                "job_builder_name": JobBuilder.name,
                "job_builder_identifier": identifier,
                "target": target,
                "reason": "render",
            },
        )
        or 0.0
    )


def _bash_payload(service: MagicMock, script: str) -> Any:
    """Build a real bash payload bound to nothing but *service*."""
    from courier.plugins.payloads.bash_payload import BashPayload

    return BashPayload(service, {"script": script}, identifier=DEFAULT_PAYLOAD_ID)


def _meta_file(name: str, **metadata: Any) -> FrozenFile:
    return FrozenFile(file=Path(f"/data/{name}.nc"), hostname="h", metadata=metadata)


def _published_scripts(service: MagicMock) -> list[str | None]:
    return [
        Job.from_string(call.kwargs["message"]).payload.script  # type: ignore[union-attr]
        for call in service.emit.call_args_list
    ]


def _failing_payload(exc: Exception) -> MagicMock:
    payload = MagicMock(spec=Payload, identifier=DEFAULT_PAYLOAD_ID)
    payload.to_job_spec.side_effect = exc
    return payload


class TestPayloadRenderFailure:
    """A pass-one render failure costs its own job and nothing else.

    Rendering depends on the job's data, so one file can fail a template the
    rest satisfy. The failure used to escape ``emit`` and reach ``os._exit``
    on the consumer thread, or kill the reaper or state-sync thread, or fail
    ``start()`` during hydration, dropping the popped job every time.
    """

    def test_nothing_is_published_and_each_target_is_counted(
        self,
        service: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        builder = _builder(service, identifier="jb-render-stub")
        builder.payload = _failing_payload(RuntimeError("template blew up"))
        targets = ["dp-a", "dp-b"]
        before = {t: _render_failures("jb-render-stub", t) for t in targets}

        with _captured(builder, caplog):
            handled = builder.emit(Job("n", "job-1", {}, files=[_file("a")]), targets)

        # Dropped rather than returned to the group: the same files would
        # fail the same render on the next pass.
        assert handled is True
        service.emit.assert_not_called()
        for target in targets:
            assert _render_failures("jb-render-stub", target) == before[target] + 1
        [record] = [r for r in caplog.records if "failed to render" in r.getMessage()]
        assert record.levelno == logging.ERROR
        assert "job-1" in record.getMessage()
        assert "/data/a.nc" in record.getMessage()
        assert record.exc_info is not None

    def test_render_happens_before_any_emit_claim_is_taken(
        self,
        service: MagicMock,
    ) -> None:
        """No claim is taken, so a peer is never locked out of the job."""
        builder = _builder(service)
        builder._sync = MagicMock()
        builder.payload = _failing_payload(ValueError("bad template"))

        builder.emit(Job("n", "job-1", {}), ["dp-a"])

        builder._sync.try_claim_emit.assert_not_called()
        service.emit.assert_not_called()

    def test_a_missing_metadata_key_fails_only_its_own_job(
        self,
        service: MagicMock,
    ) -> None:
        """The consume loop keeps going and later jobs still publish.

        Pass one renders with StrictUndefined semantics, so a key one file
        lacks raises at the builder rather than rendering as an empty string.
        """
        builder = _builder(service, identifier="jb-render-meta")
        builder.job_groups = [_OneFileGroup()]
        builder.payload = _bash_payload(service, "echo {{ files[0].metadata.sector }}")
        service.consume.return_value = iter(
            [
                (str(_meta_file("a", sector="meso")), None),
                (str(_meta_file("b")), None),
                (str(_meta_file("c", sector="full")), None),
            ],
        )
        before = _render_failures("jb-render-meta")

        with patch("courier.interfaces.job_builders.os._exit") as exit_:
            builder._run_handle_incoming_files()

        exit_.assert_not_called()
        assert _published_scripts(service) == ["echo meso", "echo full"]
        assert _render_failures("jb-render-meta") == before + 1
        assert builder.job_groups[0].jobs == {}

    def test_a_template_error_on_one_files_data_fails_only_that_job(
        self,
        service: MagicMock,
    ) -> None:
        """An expression that raises for one file's values is contained too."""
        builder = _builder(service, identifier="jb-render-type")
        builder.job_groups = [_OneFileGroup()]
        builder.payload = _bash_payload(service, "echo {{ files[0].metadata.n + 1 }}")
        service.consume.return_value = iter(
            [
                (str(_meta_file("a", n=1)), None),
                (str(_meta_file("b", n="not a number")), None),
                (str(_meta_file("c", n=2)), None),
            ],
        )
        before = _render_failures("jb-render-type")

        with patch("courier.interfaces.job_builders.os._exit") as exit_:
            builder._run_handle_incoming_files()

        exit_.assert_not_called()
        assert _published_scripts(service) == ["echo 2", "echo 3"]
        assert _render_failures("jb-render-type") == before + 1

    def test_the_reaper_path_survives(self, service: MagicMock) -> None:
        """The timeout reapers and the merge callback all go through here."""
        builder = _builder(service, identifier="jb-render-reap")
        builder.payload = _failing_payload(TypeError("boom"))
        group = builder.job_groups[0]
        group.add_file(_file("a"))
        group.add_file(_file("b"))
        before = _render_failures("jb-render-reap")

        builder._emit_ready_jobs(group, reason="hit its window")

        service.emit.assert_not_called()
        assert group.jobs == {}
        assert _render_failures("jb-render-reap") == before + 1

    def test_startup_hydration_survives(self, service: MagicMock) -> None:
        """A job already complete in shared state cannot fail ``start()``."""
        builder = _builder(service, identifier="jb-render-hydrate")
        builder._sync = MagicMock()
        builder.payload = _failing_payload(TypeError("boom"))
        group = builder.job_groups[0]
        group.add_file(_file("a"))
        group.add_file(_file("b"))
        service.consume.return_value = iter(())

        builder.start()
        try:
            assert builder.is_healthy() is True
        finally:
            builder.stop()

        service.emit.assert_not_called()
        assert group.jobs == {}

    def test_only_published_jobs_are_reported_as_emitted(
        self,
        service: MagicMock,
    ) -> None:
        """The reapers count what ``_emit_ready_jobs`` returns as timeout emits."""
        builder = _builder(service, identifier="jb-render-count")
        builder.payload = _bash_payload(service, "echo {{ files[0].metadata.sector }}")
        group = builder.job_groups[0]
        for job_id, metadata in (("bad", {}), ("good", {"sector": "meso"})):
            job = _CountingJob("n", job_id, {})
            job.add_file(_meta_file(f"{job_id}-1", **metadata))
            job.add_file(_meta_file(f"{job_id}-2", **metadata))
            group.jobs[job_id] = job

        emitted = builder._emit_ready_jobs(group, reason="hit its window")

        assert [job.identifier for job in emitted] == ["good"]
        assert _published_scripts(service) == ["echo meso"]
        assert group.jobs == {}

    def test_file_path_does_not_trace_or_count_a_dropped_job(
        self,
        service: MagicMock,
    ) -> None:
        builder = _builder(service, identifier="jb-render-built")
        builder.payload = _failing_payload(TypeError("boom"))
        group = builder.job_groups[0]
        labels = {
            "job_builder_name": JobBuilder.name,
            "job_builder_identifier": "jb-render-built",
            "status": "ready",
        }
        built = "courier_job_builder_jobs_built_total"
        before = REGISTRY.get_sample_value(built, labels) or 0.0
        span = MagicMock()

        with patch(
            "courier.interfaces.job_builders.get_current_span", return_value=span
        ):
            builder._process_job_group(group, _file("a"))
            builder._process_job_group(group, _file("b"))

        events = [call.args[0] for call in span.add_event.call_args_list]
        assert "job.ready" in events
        assert "job.emitted" not in events
        assert (REGISTRY.get_sample_value(built, labels) or 0.0) == before
        assert _render_failures("jb-render-built") == 1.0

    def test_the_error_line_lists_a_bounded_number_of_files(
        self,
        service: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A large job must not produce a log line a log store rejects."""
        builder = _builder(service, identifier="jb-render-long")
        builder.payload = _failing_payload(RuntimeError("template blew up"))
        job = Job("n", "job-1", {}, files=[_file(f"f{i:04d}") for i in range(5000)])

        with _captured(builder, caplog):
            builder.emit(job, ["dp-1"])

        [record] = [r for r in caplog.records if "failed to render" in r.getMessage()]
        message = record.getMessage()
        assert "Files (5000):" in message
        assert "and 4990 more" in message
        assert "/data/f0009.nc" in message
        assert "/data/f0010.nc" not in message
        assert len(message) < 2000  # noqa: PLR2004
