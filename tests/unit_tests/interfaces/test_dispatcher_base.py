"""Behavioural tests for the ``Dispatcher`` base class.

The consume/execute/ack loop had no dedicated tests. Two production failure
modes live here: a non-``CourierError`` escaping ``get_execution_log`` takes
the process down via ``os._exit`` with the message unacked (so it recurs on
redelivery), and the dedupe LRU silently discards any job whose identifier it
has seen before.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from courier.constants import PluginRunState, job_ready_queue_for
from courier.errors import CourierError, PipelineError
from courier.interfaces.module_based.dispatchers import (
    _DEDUPE_LRU_SIZE,
    Dispatcher,
)
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job


class _RecordingDispatcher(Dispatcher):
    """Dispatcher that records the jobs it was asked to execute."""

    name = "recording_dispatcher"
    version = "test"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.executed: list[Job] = []
        self.raise_on_execute: Exception | None = None

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        self.executed.append(job)
        return [ExecutionLog(return_code=0, stdout="ok", stderr="", hostname="h")]


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _dispatcher(service: MagicMock, identifier: str) -> _RecordingDispatcher:
    return _RecordingDispatcher(service, {}, identifier=identifier)


def _feed(dispatcher: _RecordingDispatcher, service: MagicMock, *jobs: Job) -> None:
    """Run one pass of the consume loop over *jobs*, then stop.

    The stop event is set as the stream ends rather than up front: the loop
    checks it *before* consuming, so pre-setting it would skip the batch
    entirely and the test would assert against an untouched dispatcher.
    """

    def _consume(*_args: object, **_kwargs: object):
        for job in jobs:
            yield str(job), None
        dispatcher._stop_event.set()

    service.consume.side_effect = _consume
    dispatcher.handle_incoming_jobs()


# ── construction ────────────────────────────────────────────────────────────


class TestConstruction:
    def test_identifier_is_required(self, service: MagicMock) -> None:
        """A dispatcher without an identifier has no queue to consume from."""
        with pytest.raises(ValueError, match="requires an identifier"):
            _RecordingDispatcher(service, {})

    def test_consumes_from_its_own_queue(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "runner-a")
        assert dispatcher.incoming_queue == job_ready_queue_for("runner-a")


# ── the consume loop ────────────────────────────────────────────────────────


class TestJobExecution:
    def test_consumed_job_is_executed(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "exec-basic")
        _feed(dispatcher, service, _job("job-1"))

        assert [j.identifier for j in dispatcher.executed] == ["job-1"]

    def test_job_files_survive_the_broker_round_trip(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _dispatcher(service, "exec-files")
        _feed(dispatcher, service, _job("job-1"))

        (executed,) = dispatcher.executed
        assert {str(f.file) for f in executed.files} == {"/d/a.nc"}

    def test_execution_log_is_published(self, service: MagicMock) -> None:
        """Downstream consumers read execution logs off the dispatcher queue."""
        dispatcher = _dispatcher(service, "exec-log")
        _feed(dispatcher, service, _job("job-1"))

        published = [
            ExecutionLog.from_string(call.kwargs["message"])
            for call in service.emit.call_args_list
        ]
        assert [log.return_code for log in published] == [0]

    def test_courier_error_is_contained(self, service: MagicMock) -> None:
        """A failing job must not stop the dispatcher consuming the next one."""
        dispatcher = _dispatcher(service, "exec-error")
        dispatcher.raise_on_execute = PipelineError("bad job")
        labels = {
            "status": "failure",
            "dispatcher_name": dispatcher.name,
            "dispatcher_identifier": "exec-error",
        }
        # Prometheus counters are process-global; assert the delta rather than
        # an absolute, which would depend on test ordering within the session.
        before = REGISTRY.get_sample_value(
            "courier_dispatcher_jobs_processed_total", labels,
        ) or 0.0

        _feed(dispatcher, service, _job("job-1"))  # must not raise

        after = REGISTRY.get_sample_value(
            "courier_dispatcher_jobs_processed_total", labels,
        )
        assert after == before + 1

    def test_non_courier_error_propagates_to_the_supervisor(
        self,
        service: MagicMock,
    ) -> None:
        """Unexpected errors must surface, not be swallowed into a silent stall.

        ``handle_incoming_jobs`` only catches ``CourierError``; anything else
        reaches ``_run_handle_incoming_jobs``, which logs and exits the
        process. Asserting it escapes here pins that contract — a bare
        ``except Exception`` added later would hide poison messages instead.
        """
        dispatcher = _dispatcher(service, "exec-fatal")
        dispatcher.raise_on_execute = ValueError("malformed metric line")

        with pytest.raises(ValueError, match="malformed metric line"):
            _feed(dispatcher, service, _job("job-1"))


# ── dedupe ──────────────────────────────────────────────────────────────────


class TestDedupe:
    def test_repeated_identifier_is_skipped(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "dedupe-basic")
        _feed(dispatcher, service, _job("same-id"), _job("same-id"))

        assert len(dispatcher.executed) == 1

    def test_distinct_identifiers_both_run(self, service: MagicMock) -> None:
        """The guard must not swallow genuinely different jobs."""
        dispatcher = _dispatcher(service, "dedupe-distinct")
        _feed(dispatcher, service, _job("id-a"), _job("id-b"))

        assert [j.identifier for j in dispatcher.executed] == ["id-a", "id-b"]

    def test_skip_is_counted(self, service: MagicMock) -> None:
        """A dropped duplicate must be visible in metrics, not silent."""
        dispatcher = _dispatcher(service, "dedupe-counted")
        labels = {"dispatcher_identifier": "dedupe-counted"}
        before = REGISTRY.get_sample_value(
            "courier_dispatcher_dedupe_skips_total", labels,
        ) or 0.0

        _feed(dispatcher, service, _job("dup"), _job("dup"))

        after = REGISTRY.get_sample_value(
            "courier_dispatcher_dedupe_skips_total", labels,
        )
        assert after == before + 1

    def test_lru_is_bounded(self, service: MagicMock) -> None:
        """An unbounded set would grow without limit on a long-running node."""
        dispatcher = _dispatcher(service, "dedupe-bounded")
        for index in range(_DEDUPE_LRU_SIZE + 50):
            dispatcher._recently_seen(f"job-{index}")

        assert len(dispatcher._seen_jobs) <= _DEDUPE_LRU_SIZE

    def test_oldest_entry_is_evicted_first(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "dedupe-evict")
        for index in range(_DEDUPE_LRU_SIZE + 1):
            dispatcher._recently_seen(f"job-{index}")

        assert dispatcher._recently_seen("job-0") is False, "oldest should be gone"
        assert dispatcher._recently_seen(f"job-{_DEDUPE_LRU_SIZE}") is True


# ── lifecycle ───────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_stop_signals_the_consume_loop(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "life-stop")
        service.consume.return_value = iter(())

        dispatcher.start()
        assert dispatcher.is_healthy() is True
        dispatcher.stop()

        assert dispatcher._stop_event.is_set()
        assert dispatcher._state is PluginRunState.STOPPED
        assert not (
            dispatcher._main_thread and dispatcher._main_thread.is_alive()
        )

    def test_consume_receives_the_stop_event(self, service: MagicMock) -> None:
        """The stop event must reach the broker loop, not just be stored."""
        dispatcher = _dispatcher(service, "life-event")
        _feed(dispatcher, service)  # empty stream, stops after one pass

        assert (
            service.consume.call_args.kwargs["stop_event"]
            is dispatcher._stop_event
        )
        assert service.consume.call_args[0][0] == dispatcher.incoming_queue

    def test_emit_file_feeds_the_found_file_exchange(
        self,
        service: MagicMock,
    ) -> None:
        """Chained pipelines depend on dispatcher output re-entering the front."""
        from courier.constants import FILE_FOUND_EXCHANGE

        dispatcher = _dispatcher(service, "life-emit-file")
        dispatcher.emit_file(File(file=Path("/out/product.nc")))

        assert service.emit.call_args.kwargs["queue"] == FILE_FOUND_EXCHANGE
        emitted = File.from_string(service.emit.call_args.kwargs["message"])
        assert str(emitted.file) == "/out/product.nc"
