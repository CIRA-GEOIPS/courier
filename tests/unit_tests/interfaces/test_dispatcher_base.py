"""Behavioural tests for the ``Dispatcher`` base class.

The consume/execute/ack loop is where one bad job could once take a whole
service down: any non-``CourierError`` escaping ``get_execution_log`` reached
``os._exit`` with the message unacked, so it recurred on every redelivery.
These tests pin the contract that replaced that:

* a job that fails while being prepared or run is contained and counted;
* a message this dispatcher cannot execute at all -- not a job, no payload,
  an unknown or incompatible payload, an invalid spec, a missing toolchain --
  is parked on the dead-letter queue rather than acknowledged and dropped;
* only a broker-level fault (or a failure to park) still escapes the loop.

The dispatcher hydrates the payload a job carries rather than pairing with
one, so the compatibility tests build payload specs directly.
"""

# cspell:ignore nosuch Glzc hlcg

from __future__ import annotations

import contextlib
import json
import logging
import stat
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import MagicMock, patch

import kombu.exceptions
import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState, job_ready_queue_for
from courier.errors import (
    CourierError,
    FatalBrokerError,
    PipelineError,
    TransientBrokerError,
    UnexecutableJobError,
)
from courier.interfaces.dispatchers import (
    _DEDUPE_LRU_SIZE,
    Dispatcher,
    ExecutionPayload,
)
from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job
from courier.types.payload import PayloadSpec

if TYPE_CHECKING:
    from collections.abc import Iterator


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


class _OutputFilesDispatcher(_RecordingDispatcher):
    """A dispatcher whose job prints two output file paths."""

    name = "output_files_dispatcher"

    def get_execution_log(self, job: Job) -> list[ExecutionLog]:
        self.executed.append(job)
        return [
            ExecutionLog(
                return_code=0,
                stdout="/d/one.nc\n/d/two.nc\n",
                stderr="",
                hostname="h",
            ),
        ]


class _CodeEmitsFilesDispatcher(_RecordingDispatcher):
    """Decides in code which files a job produced (the documented recipe)."""

    name = "code_emits_files_dispatcher"

    def _collect_output_files(
        self,
        job: Job,
        logs: list[ExecutionLog],
    ) -> list[File]:
        files = super()._collect_output_files(job, logs)
        if all(log.return_code == 0 for log in logs):
            files.extend(
                File(file=Path("/l2") / f"{Path(source.file).stem}_cal.nc")
                for source in job.files
            )
        return files


class _ForeignPayload(Payload):
    """A payload representation no shipped dispatcher can run."""

    name = "foreign_payload"


class _ForeignOnlyDispatcher(LocalDispatcher):
    """A dispatcher that can run nothing but ``_ForeignPayload``."""

    name = "foreign_only_dispatcher"
    representations: ClassVar[list[type[Payload]]] = [_ForeignPayload]


class _CustomBashPayload(BashPayload):
    """A third-party payload that shipped dispatchers run as ``BashPayload``."""

    name = "custom_bash_payload"


class _OptionedConfig(DispatcherGroupConfig):
    """Config model of a dispatcher with an option of its own."""

    queue_hint: int = 0


class _OptionedDispatcher(_RecordingDispatcher):
    """A dispatcher whose config model adds ``queue_hint``."""

    name = "optioned_dispatcher"
    config_class: ClassVar[type[DispatcherGroupConfig]] = _OptionedConfig


def _job(identifier: str = "job-1", payload: PayloadSpec | None = None) -> Job:
    return Job(
        "n",
        identifier,
        {},
        files=[File(file=Path("/d/a.nc")).freeze()],
        payload=payload,
    )


def _spec(name: str, identifier: str = "payload-1", **config: object) -> PayloadSpec:
    return PayloadSpec(name=name, identifier=identifier, config=config)


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def private_tmpdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``TMPDIR`` at an empty directory, as a deployment would.

    ``tempfile`` caches the temp directory on first use, so the cache is
    cleared too; monkeypatch restores both afterwards.
    """
    directory = tmp_path / "tmpdir"
    directory.mkdir()
    monkeypatch.setenv("TMPDIR", str(directory))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return directory


def _dispatcher(service: MagicMock, identifier: str) -> _RecordingDispatcher:
    return _RecordingDispatcher(service, {}, identifier=identifier)


def _feed(dispatcher: Dispatcher, service: MagicMock, *jobs: Job | str) -> None:
    """Run the consume loop over *jobs* (jobs or raw message bodies), then stop.

    ``Service.consume`` only returns once the stop event is set, so the fake
    stream sets it after the last message to mirror that; the loop itself does
    not consult the event.
    """

    def _consume(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        for job in jobs:
            yield str(job), None
        dispatcher._stop_event.set()

    service.consume.side_effect = _consume
    dispatcher.handle_incoming_jobs()


def _processed(dispatcher: Dispatcher, status: str) -> float:
    """Return the ``jobs_processed`` sample for *dispatcher* and *status*.

    Prometheus counters are process-global; tests assert deltas of this value
    rather than absolutes, which would depend on test ordering.
    """
    return (
        REGISTRY.get_sample_value(
            "courier_dispatcher_jobs_processed_total",
            {
                "status": status,
                "dispatcher_name": dispatcher.name,
                "dispatcher_identifier": dispatcher.identifier,
            },
        )
        or 0.0
    )


def _bash_job(identifier: str = "job-1", **config: object) -> Job:
    """Return a job carrying an executable ``bash_payload`` spec."""
    return _job(identifier, _spec("bash_payload", **config))


@contextlib.contextmanager
def _captured(logger_name: str) -> Iterator[list[logging.LogRecord]]:
    """Collect the records *logger_name* emits, whatever its current setup.

    Courier loggers do not propagate, so ``caplog`` cannot see them, and their
    level depends on whichever config configured them first.  Attaching a
    handler directly and forcing the level keeps the capture independent of
    test order.
    """
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = _Collector(level=logging.DEBUG)
    previous_level, previous_disabled = logger.level, logger.disabled
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.disabled = previous_disabled


# ── construction ────────────────────────────────────────────────────────────


class TestConstruction:
    def test_identifier_is_required(self, service: MagicMock) -> None:
        """A dispatcher without an identifier has no queue to consume from."""
        with pytest.raises(ValueError, match="requires an identifier"):
            _RecordingDispatcher(service, {})

    def test_consumes_from_its_own_queue(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "runner-a")
        assert dispatcher.incoming_queue == job_ready_queue_for("runner-a")

    def test_config_is_validated_with_the_config_class(
        self,
        service: MagicMock,
    ) -> None:
        """Subclasses declare their config model instead of re-validating."""
        dispatcher = LocalDispatcher(service, {}, identifier="cfg")
        assert isinstance(dispatcher.config, LocalDispatcher.config_class)

    def test_subclass_options_validate_against_the_subclass_model(
        self,
        service: MagicMock,
    ) -> None:
        """Unknown keys are rejected, so a subclass's own keys need its model.

        The base ``__init__`` once validated every dispatcher's config against
        ``DispatcherGroupConfig``, which rejects a subclass option (Slurm's
        ``slurm_output_dir``) as an unknown key.
        """
        dispatcher = _OptionedDispatcher(service, {"queue_hint": 3}, identifier="o")

        assert isinstance(dispatcher.config, _OptionedConfig)
        assert dispatcher.config.queue_hint == 3  # noqa: PLR2004
        with pytest.raises(ValidationError, match="queue_hint"):
            _RecordingDispatcher(service, {"queue_hint": 3}, identifier="o")

    def test_unknown_key_is_rejected(self, service: MagicMock) -> None:
        """A typo'd option must fail at startup, not be silently ignored."""
        with pytest.raises(ValidationError, match="timeout_second"):
            LocalDispatcher(service, {"timeout_second": 5}, identifier="cfg")


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
        before = _processed(dispatcher, "failure")

        _feed(dispatcher, service, _job("job-1"))  # must not raise

        assert _processed(dispatcher, "failure") == before + 1
        service.park_message.assert_not_called()

    def test_non_courier_error_is_contained(self, service: MagicMock) -> None:
        """A stray exception from one job fails that job, not the process.

        It used to escape to ``_run_handle_incoming_jobs`` and ``os._exit``
        with the message unacked, so the same job killed the service again on
        every redelivery.
        """
        dispatcher = _dispatcher(service, "exec-stray")
        dispatcher.raise_on_execute = ValueError("malformed metric line")
        before = _processed(dispatcher, "failure")

        _feed(dispatcher, service, _job("job-1"))  # must not raise

        assert _processed(dispatcher, "failure") == before + 1

    def test_next_job_runs_after_a_stray_error(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "exec-stray-next")
        calls: list[str] = []

        def _flaky(job: Job) -> list[ExecutionLog]:
            calls.append(job.identifier)
            if job.identifier == "job-1":
                raise KeyError("boom")
            return [ExecutionLog(return_code=0, stdout="", stderr="", hostname="h")]

        with patch.object(dispatcher, "get_execution_log", side_effect=_flaky):
            _feed(dispatcher, service, _job("job-1"), _job("job-2"))

        assert calls == ["job-1", "job-2"]

    def test_broker_failure_publishing_results_escapes(
        self,
        service: MagicMock,
    ) -> None:
        """A broker fault is not one job's problem; it must reach the supervisor.

        ``_run_handle_incoming_jobs`` then exits, leaving the message unacked
        for redelivery, which is the only safe outcome when the broker itself
        is failing.
        """
        dispatcher = _dispatcher(service, "exec-broker")
        service.emit.side_effect = ConnectionError("broker gone")

        with pytest.raises(ConnectionError, match="broker gone"):
            _feed(dispatcher, service, _job("job-1"))

    def test_broker_failure_re_emitting_output_files_escapes(
        self,
        service: MagicMock,
    ) -> None:
        """Output files are published like the results: a fault is redelivered.

        Converting it into a job failure would ack the message and drop the
        chained pipeline's downstream files for good.
        """
        dispatcher = _OutputFilesDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/\S+\.nc)"}]},
            identifier="exec-broker-files",
        )
        failures_before = _processed(dispatcher, "failure")

        def _emit(queue: str, message: str) -> None:  # noqa: ARG001
            if queue == FILE_FOUND_EXCHANGE:
                raise kombu.exceptions.OperationalError("broker gone")

        service.emit.side_effect = _emit

        with pytest.raises(kombu.exceptions.OperationalError, match="broker gone"):
            _feed(dispatcher, service, _job("job-1"))

        assert _processed(dispatcher, "failure") == failures_before

    @pytest.mark.parametrize(
        "fault",
        [
            TransientBrokerError("connection dropped"),
            FatalBrokerError("access refused"),
            kombu.exceptions.OperationalError("broker gone"),
        ],
        ids=lambda fault: type(fault).__name__,
    )
    @pytest.mark.parametrize("path", ["execution log", "output files"])
    def test_publish_fault_is_redelivered_not_counted_as_a_job_failure(
        self,
        service: MagicMock,
        fault: Exception,
        path: str,
    ) -> None:
        """A broker fault while publishing a job's results is not the job's.

        ``TransientBrokerError`` and ``FatalBrokerError`` are CourierErrors,
        so they used to be caught by the job-level handler: the message was
        acknowledged as a failed job and its results were lost.  Now they escape the
        loop (the message stays unacked and is redelivered), the job is not
        counted as failed or succeeded, and the dedupe LRU forgets it so a
        redelivery to this instance runs it rather than skipping it.
        """
        dispatcher = _OutputFilesDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/\S+\.nc)"}]},
            identifier=f"exec-publish-{type(fault).__name__}-{path[0]}",
        )
        target = FILE_FOUND_EXCHANGE if path == "output files" else dispatcher.queue
        failures = _processed(dispatcher, "failure")
        successes = _processed(dispatcher, "success")

        def _emit(queue: str, message: str) -> None:  # noqa: ARG001
            if queue == target:
                raise fault

        service.emit.side_effect = _emit

        with pytest.raises(type(fault)):
            _feed(dispatcher, service, _job("job-1"))

        assert _processed(dispatcher, "failure") == failures
        assert _processed(dispatcher, "success") == successes
        assert not dispatcher._recently_seen(dispatcher._dedupe_key(_job("job-1")))

    def test_files_a_dispatcher_decides_in_code_are_published_after_the_job(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _CodeEmitsFilesDispatcher(service, {}, identifier="code-files")

        _feed(dispatcher, service, _job("job-1"))

        emitted = [
            json.loads(call.kwargs["message"])["file"]
            for call in service.emit.call_args_list
            if call.kwargs["queue"] == FILE_FOUND_EXCHANGE
        ]
        assert emitted == ["/l2/a_cal.nc"]

    @pytest.mark.parametrize(
        "fault",
        [TransientBrokerError("connection dropped"), FatalBrokerError("refused")],
        ids=lambda fault: type(fault).__name__,
    )
    def test_publishing_files_decided_in_code_is_redelivered_on_a_fault(
        self,
        service: MagicMock,
        fault: Exception,
    ) -> None:
        dispatcher = _CodeEmitsFilesDispatcher(
            service,
            {},
            identifier=f"code-files-{type(fault).__name__}",
        )
        failures = _processed(dispatcher, "failure")

        def _emit(queue: str, message: str) -> None:  # noqa: ARG001
            if queue == FILE_FOUND_EXCHANGE:
                raise fault

        service.emit.side_effect = _emit

        with pytest.raises(type(fault)):
            _feed(dispatcher, service, _job("job-1"))

        assert _processed(dispatcher, "failure") == failures

    def test_a_bug_in_an_output_file_override_fails_only_the_job(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _CodeEmitsFilesDispatcher(service, {}, identifier="code-bug")
        failures = _processed(dispatcher, "failure")

        with patch.object(
            _RecordingDispatcher,
            "_collect_output_files",
            side_effect=KeyError("bug"),
        ):
            _feed(dispatcher, service, _job("job-1"))  # must not raise

        assert _processed(dispatcher, "failure") == failures + 1
        service.emit.assert_not_called()

    def test_publish_fault_makes_the_supervisor_exit(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _dispatcher(service, "exec-publish-exit")
        service.emit.side_effect = TransientBrokerError("connection dropped")
        service.consume.return_value = iter([(str(_job("job-1")), None)])

        with patch("courier.interfaces.dispatchers.os._exit") as exit_:
            dispatcher._run_handle_incoming_jobs()

        exit_.assert_called_once_with(1)

    def test_job_level_courier_error_is_still_contained(
        self,
        service: MagicMock,
    ) -> None:
        """Only publishing escapes: an error of the job's own stays contained."""
        dispatcher = _dispatcher(service, "exec-job-broker-named")
        dispatcher.raise_on_execute = CourierError("the job's own failure")
        before = _processed(dispatcher, "failure")

        _feed(dispatcher, service, _job("job-1"))  # must not raise

        assert _processed(dispatcher, "failure") == before + 1
        service.emit.assert_not_called()

    def test_supervisor_exits_only_on_escaping_errors(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _dispatcher(service, "exec-supervisor")
        dispatcher.raise_on_execute = TypeError("job-level")
        service.consume.return_value = iter([(str(_job("job-1")), None)])

        with patch("courier.interfaces.dispatchers.os._exit") as exit_:
            dispatcher._run_handle_incoming_jobs()

        exit_.assert_not_called()


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

    def test_same_identifier_from_two_builders_both_run(
        self,
        service: MagicMock,
    ) -> None:
        """Two builders sharing a dispatcher mint the same id for one file.

        Each carries different work (its own payload), so the second must not
        be dropped as a duplicate of the first.
        """
        dispatcher = _dispatcher(service, "dedupe-two-builders")
        archive = _job("/d/a.nc", _spec("bash_payload", "archive", binary="true"))
        process = _job("/d/a.nc", _spec("bash_payload", "process", binary="true"))

        _feed(dispatcher, service, archive, process)

        assert [j.payload.identifier for j in dispatcher.executed if j.payload] == [
            "archive",
            "process",
        ]

    def test_redelivery_of_a_payload_job_is_skipped(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _dispatcher(service, "dedupe-redelivery")
        job = _job("job-1", _spec("bash_payload", "archive", binary="true"))

        _feed(dispatcher, service, job, job)

        assert len(dispatcher.executed) == 1

    def test_key_is_scoped_by_payload_identifier(self) -> None:
        assert Dispatcher._dedupe_key(_job("j")) == ("", "j")
        assert Dispatcher._dedupe_key(_job("j", _spec("x", "p"))) == ("p", "j")

    def test_skip_is_counted(self, service: MagicMock) -> None:
        """A dropped duplicate must be visible in metrics, not silent."""
        dispatcher = _dispatcher(service, "dedupe-counted")
        labels = {"dispatcher_identifier": "dedupe-counted"}
        before = (
            REGISTRY.get_sample_value(
                "courier_dispatcher_dedupe_skips_total",
                labels,
            )
            or 0.0
        )

        _feed(dispatcher, service, _job("dup"), _job("dup"))

        after = REGISTRY.get_sample_value(
            "courier_dispatcher_dedupe_skips_total",
            labels,
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
        assert not (dispatcher._main_thread and dispatcher._main_thread.is_alive())

    def test_consume_receives_the_stop_event(self, service: MagicMock) -> None:
        """The stop event must reach the broker loop, not just be stored."""
        dispatcher = _dispatcher(service, "life-event")
        _feed(dispatcher, service)  # empty stream, stops after one pass

        assert service.consume.call_args.kwargs["stop_event"] is dispatcher._stop_event
        assert service.consume.call_args[0][0] == dispatcher.incoming_queue

    def test_emit_file_feeds_the_found_file_exchange(
        self,
        service: MagicMock,
    ) -> None:
        """Chained pipelines depend on dispatcher output re-entering the front."""
        dispatcher = _dispatcher(service, "life-emit-file")
        dispatcher.emit_file(File(file=Path("/out/product.nc")))

        assert service.emit.call_args.kwargs["queue"] == FILE_FOUND_EXCHANGE
        emitted = File.from_string(service.emit.call_args.kwargs["message"])
        assert str(emitted.file) == "/out/product.nc"


# ── payload hydration and compatibility ─────────────────────────────────────


class TestPayloadResolution:
    """Hydration must pick the *right* representation, not just a compatible one.

    Each payload class configures a different interpreter, so asserting the
    interpreter the hydrated instance selects proves the correct class (and its
    ``_configure_from_config``) was used — unlike an ``isinstance`` check, which
    is also satisfied by the more specific subclasses.
    """

    def test_bash_payload_hydrates(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        resolved = dispatcher._resolve_job_payload(
            _job("j", _spec("bash_payload", binary="echo")),
        )

        assert type(resolved) is BashPayload
        assert resolved.generate_calling_method() == ["bash", "-c"]

    def test_shell_payload_hydrates(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        resolved = dispatcher._resolve_job_payload(
            _job("j", _spec("shell_payload", binary="echo")),
        )

        assert type(resolved) is ShellPayload
        assert resolved.generate_calling_method() == ["sh", "-c"]

    def test_most_specific_representation_wins(self, service: MagicMock) -> None:
        """A python payload hydrates as python even where shell would run it."""
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        resolved = dispatcher._resolve_job_payload(
            _job("j", _spec("python_payload", binary="echo")),
        )

        assert type(resolved) is PythonPayload
        assert resolved.generate_calling_method() == ["python", "-c"]

    def test_hydrated_payload_gets_the_dispatcher_config(
        self,
        service: MagicMock,
    ) -> None:
        """Timeouts and logging flags come from the dispatcher that runs the job."""
        dispatcher = LocalDispatcher(
            service,
            {"timeout_seconds": 7},
            identifier="celebrant",
        )
        resolved = dispatcher._resolve_job_payload(
            _job("j", _spec("bash_payload", binary="echo")),
        )

        assert resolved.base_config is dispatcher.config

    def test_lowering_to_a_listed_representation_is_logged_once(
        self,
        service: MagicMock,
    ) -> None:
        """A payload run as a base class loses its own command generation.

        That used to happen silently; it is now logged the first time.
        """
        dispatcher = LocalDispatcher(service, {}, identifier="lowering")
        job = _job("j", _spec("custom_bash_payload", binary="echo"))
        with (
            patch("courier.interfaces.dispatchers.payloads") as registry,
            _captured("courier.plugin.local_dispatcher") as records,
        ):
            registry.get_plugin.return_value = _CustomBashPayload
            first = dispatcher._resolve_job_payload(job)
            second = dispatcher._resolve_job_payload(job)

        assert type(first) is BashPayload
        assert type(second) is BashPayload
        assert first.payload_name == "custom_bash_payload"
        lowered = [
            r.getMessage()
            for r in records
            if r.levelno == logging.INFO and "as BashPayload" in r.getMessage()
        ]
        assert len(lowered) == 1
        assert "'custom_bash_payload'" in lowered[0]
        registry.get_plugin.assert_called_once_with("custom_bash_payload")

    def test_own_representation_is_not_reported_as_lowering(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="lowering")
        with _captured("courier.plugin.local_dispatcher") as records:
            dispatcher._resolve_job_payload(
                _job("j", _spec("bash_payload", binary="echo")),
            )

        assert not [r for r in records if "runs payload" in r.getMessage()]

    def test_missing_payload_is_unexecutable(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        with pytest.raises(UnexecutableJobError, match="carries no payload"):
            dispatcher._resolve_job_payload(_job("j"))

    @pytest.mark.parametrize("suffix", ["/../../x.sh", "a/b", ".sh\\x", ".s\x00h"])
    def test_suffix_with_a_separator_is_unexecutable(
        self,
        service: MagicMock,
        suffix: str,
    ) -> None:
        """The suffix is part of a file name; it must not choose the directory."""
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        spec = PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"script": "x"},
            script="echo hi",
            suffix=suffix,
        )
        with pytest.raises(UnexecutableJobError, match="script suffix"):
            dispatcher._resolve_job_payload(_job("j", spec))

    def test_incompatible_payload_is_unexecutable(self, service: MagicMock) -> None:
        dispatcher = _ForeignOnlyDispatcher(service, {}, identifier="celebrant")
        with pytest.raises(UnexecutableJobError, match="no compatible representation"):
            dispatcher._resolve_job_payload(
                _job("j", _spec("bash_payload", binary="echo")),
            )

    def test_unknown_payload_plugin_is_unexecutable(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        with pytest.raises(UnexecutableJobError, match="'nope_payload'"):
            dispatcher._resolve_job_payload(
                _job("j", _spec("nope_payload", binary="echo")),
            )

    @pytest.mark.parametrize(
        "config",
        [
            pytest.param({}, id="no-file-script-or-binary"),
            pytest.param({"binary": "echo", "toolchain": 5}, id="wrong-type"),
        ],
    )
    def test_invalid_payload_config_is_unexecutable(
        self,
        service: MagicMock,
        config: dict[str, object],
    ) -> None:
        """A pydantic ValidationError from the job's spec is not a CourierError.

        It must still be reported as one, or it escapes the consume loop.
        """
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        with pytest.raises(UnexecutableJobError, match="invalid 'bash_payload'"):
            dispatcher._resolve_job_payload(_job("j", _spec("bash_payload", **config)))

    def test_unavailable_toolchain_is_unexecutable(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        job = _job(
            "j",
            _spec(
                "bash_payload",
                binary="echo",
                toolchain=["courier-test-no-such-tool"],
            ),
        )
        with pytest.raises(UnexecutableJobError, match="Toolchain validation failed"):
            dispatcher._resolve_job_payload(job)

    def test_toolchain_probe_error_is_unexecutable(self, service: MagicMock) -> None:
        """A probe that raises (e.g. a logging misconfiguration) is contained."""
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        job = _job("j", _spec("bash_payload", binary="echo", toolchain=["sh"]))
        with (
            patch.object(
                BashPayload,
                "validate_toolchain_arg",
                side_effect=ValueError("log_file_path missing"),
            ),
            pytest.raises(UnexecutableJobError, match="log_file_path missing"),
        ):
            dispatcher._resolve_job_payload(job)

    def test_toolchain_success_is_cached_and_failure_is_not(
        self,
        service: MagicMock,
    ) -> None:
        """A host fixed after a failed probe is picked up without a restart."""
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        job = _job("j", _spec("bash_payload", binary="echo", toolchain=["sh"]))
        ok = [ExecutionLog(return_code=0, stdout="/bin/sh", stderr="", hostname="h")]
        missing = [ExecutionLog(return_code=1, stdout="", stderr="", hostname="h")]
        with patch.object(
            BashPayload,
            "validate_toolchain_arg",
            side_effect=[missing, ok, AssertionError("probed again")],
        ) as probe:
            with pytest.raises(UnexecutableJobError):
                dispatcher._resolve_job_payload(job)
            dispatcher._resolve_job_payload(job)
            dispatcher._resolve_job_payload(job)

        assert probe.call_count == 2  # noqa: PLR2004


class TestCompatibilityApi:
    """Compatibility is answerable from the class, for startup checks."""

    def test_compatible_representation_is_a_classmethod(self) -> None:
        assert LocalDispatcher.compatible_representation(PythonPayload) is PythonPayload
        assert LocalDispatcher.compatible_representation(_ForeignPayload) is None
        assert (
            _ForeignOnlyDispatcher.compatible_representation(_ForeignPayload)
            is _ForeignPayload
        )

    def test_instance_calls_still_work(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="compat")
        assert dispatcher.compatible_representation(BashPayload) is BashPayload

    def test_representation_names(self, service: MagicMock) -> None:
        names = "ShellPayload, BashPayload, PythonPayload"
        assert LocalDispatcher.representation_names() == names
        dispatcher = LocalDispatcher(service, {}, identifier="compat")
        assert dispatcher.supported_representations == names
        assert Dispatcher.representation_names() == "(none)"


# ── unexecutable jobs are parked, not dropped ───────────────────────────────


def _body_without_payload(identifier: str = "job-old") -> str:
    """Return a job message as a builder from before payloads would send it."""
    body = json.loads(str(_job(identifier)))
    body.pop("payload")
    return json.dumps(body)


def _body_with(**fields: object) -> str:
    """Return a job message with *fields* overwritten, as a buggy peer might."""
    return json.dumps(
        {**json.loads(str(_bash_job("job-odd", binary="true"))), **fields}
    )


class TestUnexecutableJobsAreParked:
    """A job this dispatcher cannot run is kept for a re-drive, never dropped."""

    @pytest.mark.parametrize(
        ("body", "reason"),
        [
            pytest.param(
                _body_without_payload(), "carries no payload", id="no-payload"
            ),
            pytest.param(
                str(_job("j-unknown", _spec("nope_payload", binary="echo"))),
                "'nope_payload' is not available",
                id="unknown-payload",
            ),
            pytest.param(
                str(_job("j-invalid", _spec("bash_payload"))),
                "invalid 'bash_payload' payload spec",
                id="invalid-spec",
            ),
            pytest.param("this is not json", "not a valid job", id="not-json"),
            pytest.param('{"name": "n"}', "not a valid job", id="missing-keys"),
            pytest.param(
                json.dumps(
                    {**json.loads(str(_job("j-nameless"))), "payload": {"config": {}}},
                ),
                "not a valid job",
                id="payload-without-name",
            ),
            pytest.param(
                _body_with(identifier=["a", "b"]),
                "'identifier' must be a string",
                id="unhashable-identifier",
            ),
            pytest.param(
                _body_with(last_modified="yesterday"),
                "'last_modified' is not a timestamp",
                id="string-last-modified",
            ),
            pytest.param(
                _body_with(last_modified=10**400),
                "'last_modified' is not a timestamp",
                id="huge-last-modified",
            ),
            pytest.param(
                _body_with(emit_time="now"),
                "'emit_time' is not a timestamp",
                id="string-emit-time",
            ),
            pytest.param(
                _body_with(payload={"name": "bash_payload", "identifier": 5}),
                "not a valid job",
                id="payload-identifier-not-a-string",
            ),
        ],
    )
    def test_message_is_parked_with_its_reason(
        self,
        service: MagicMock,
        body: str,
        reason: str,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        before = _processed(dispatcher, "unexecutable")

        _feed(dispatcher, service, body)

        service.park_message.assert_called_once()
        queue, parked_body, parked_reason = service.park_message.call_args.args
        assert queue == dispatcher.incoming_queue
        assert parked_body == body, "the message must be parked verbatim"
        assert reason in parked_reason
        assert _processed(dispatcher, "unexecutable") == before + 1
        assert all(
            call.kwargs.get("queue") != dispatcher.queue
            for call in service.emit.call_args_list
        ), "an unexecuted job must not publish an execution log"

    def test_incompatible_representation_is_parked(self, service: MagicMock) -> None:
        dispatcher = _ForeignOnlyDispatcher(service, {}, identifier="parker")
        _feed(dispatcher, service, _bash_job(binary="true"))

        (_, _, reason) = service.park_message.call_args.args
        assert "no compatible representation" in reason

    def test_missing_toolchain_is_parked(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        _feed(
            dispatcher,
            service,
            _bash_job(binary="true", toolchain=["courier-test-no-such-tool"]),
        )

        (_, _, reason) = service.park_message.call_args.args
        assert "courier-test-no-such-tool" in reason

    def test_loop_continues_after_parking(self, service: MagicMock) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        _feed(
            dispatcher,
            service,
            _body_without_payload("job-old"),
            _bash_job("job-new", binary="true"),
        )

        service.park_message.assert_called_once()
        published = [
            ExecutionLog.from_string(call.kwargs["message"])
            for call in service.emit.call_args_list
            if call.kwargs.get("queue") == dispatcher.queue
        ]
        assert [log.return_code for log in published] == [0]

    def test_park_failure_propagates(self, service: MagicMock) -> None:
        """If parking fails the message must stay unacked, so the error escapes."""
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        service.park_message.side_effect = ConnectionError("dead-letter queue gone")

        with pytest.raises(ConnectionError, match="dead-letter queue gone"):
            _feed(dispatcher, service, _body_without_payload())

    def test_parked_job_is_not_remembered_as_seen(self, service: MagicMock) -> None:
        """A job re-driven after the host is fixed must run, not be deduped."""
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        job = _bash_job("job-1", binary="true")
        with patch.object(
            LocalDispatcher,
            "_validate_payload_toolchain",
            side_effect=[UnexecutableJobError("tool missing"), None],
        ):
            _feed(dispatcher, service, job, job)

        service.park_message.assert_called_once()
        published = [
            call
            for call in service.emit.call_args_list
            if call.kwargs.get("queue") == dispatcher.queue
        ]
        assert len(published) == 1

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(_body_without_payload(), id="no-payload"),
            pytest.param(str(_job("j", _spec("bash_payload"))), id="invalid-spec"),
            pytest.param("{", id="not-json"),
            pytest.param(_body_with(identifier={"a": 1}), id="dict-identifier"),
            pytest.param(_body_with(emit_time=[1]), id="list-emit-time"),
        ],
    )
    def test_process_does_not_exit(self, service: MagicMock, body: str) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="parker")
        service.consume.return_value = iter([(body, None)])

        with patch("courier.interfaces.dispatchers.os._exit") as exit_:
            dispatcher._run_handle_incoming_jobs()

        exit_.assert_not_called()
        service.park_message.assert_called_once()

    def test_unexecutable_is_a_courier_error(self) -> None:
        assert issubclass(UnexecutableJobError, CourierError)


# ── containment inside get_execution_log ────────────────────────────────────


class TestContainment:
    """Every failure preparing or running a job surfaces as a CourierError."""

    def test_stray_error_while_executing_becomes_courier_error(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="contain")
        job = _job(
            "j",
            PayloadSpec(
                name="bash_payload",
                identifier="p",
                config={"script": "x"},
                script="echo hi",
            ),
        )
        with (
            patch.object(
                LocalDispatcher,
                "_execute_job",
                side_effect=ValueError("bad"),
            ),
            pytest.raises(CourierError, match="ValueError: bad") as caught,
        ):
            dispatcher.get_execution_log(job)

        assert not isinstance(caught.value, UnexecutableJobError)
        assert list(private_tmpdir.iterdir()) == [], "script must be removed"

    def test_command_render_failure_becomes_courier_error(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="contain")
        job = _job(
            "j",
            PayloadSpec(
                name="bash_payload",
                identifier="p",
                config={"script": "x", "suffix_args": ["{{ nosuch }}"]},
                script="echo hi",
            ),
        )

        with pytest.raises(CourierError, match="Failed to initialize environment"):
            dispatcher.get_execution_log(job)

        assert list(private_tmpdir.iterdir()) == [], "script must not leak"

    def test_output_scan_error_fails_only_the_job(
        self,
        service: MagicMock,
    ) -> None:
        """A bug while scanning is the job's; unlike a broker fault it is contained."""
        dispatcher = LocalDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
            identifier="contain",
        )
        failures_before = _processed(dispatcher, "failure")
        with (
            patch(
                "courier.interfaces.dispatchers._scan_and_emit_output_files",
                side_effect=TypeError("bad match"),
            ),
            pytest.raises(CourierError, match="TypeError: bad match"),
        ):
            dispatcher._collect_output_files(
                _job(),
                [ExecutionLog(return_code=0, stdout="")],
            )

        with patch(
            "courier.interfaces.dispatchers._scan_and_emit_output_files",
            side_effect=TypeError("bad match"),
        ):
            _feed(dispatcher, service, _bash_job(binary="true"))

        assert _processed(dispatcher, "failure") == failures_before + 1


# ── materializing the job script ────────────────────────────────────────────


def _script_job(script: str, *, nonce: str = "", suffix: str = ".sh") -> Job:
    return _job(
        "job-1",
        PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"script": "x"},
            script=script,
            suffix=suffix,
            defer_nonce=nonce,
        ),
    )


class TestScriptMaterialization:
    """The job script is a private, randomly named file that never leaks."""

    def test_script_is_written_to_tmpdir(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        job = _script_job("echo hi", suffix=".bash")
        payload = dispatcher._resolve_job_payload(job)

        path, context = dispatcher._materialize_script(job, payload)

        assert path is not None
        assert path.parent == private_tmpdir
        assert path.name.startswith("courier-")
        assert path.suffix == ".bash"
        assert path.read_text() == "echo hi"
        assert stat.S_IMODE(path.stat().st_mode) == 0o755  # noqa: PLR2004
        assert context["script_path"] == str(path)

    def test_directory_and_prefix_are_honoured(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        job = _script_job("echo hi")
        payload = dispatcher._resolve_job_payload(job)

        path, _ = dispatcher._materialize_script(
            job,
            payload,
            directory=tmp_path,
            prefix="job-1-",
        )

        assert path is not None
        assert path.parent == tmp_path
        assert path.name.startswith("job-1-")

    def test_each_job_gets_its_own_file(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """Fixed names let two dispatchers overwrite each other's script."""
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        job = _script_job("echo hi")
        payload = dispatcher._resolve_job_payload(job)

        first, _ = dispatcher._materialize_script(job, payload)
        second, _ = dispatcher._materialize_script(job, payload)

        assert first != second
        assert len(list(private_tmpdir.iterdir())) == 2  # noqa: PLR2004

    def test_subclass_can_adjust_the_written_text(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """A batch-submitting dispatcher can, e.g., ensure a shebang."""

        class _Shebang(LocalDispatcher):
            name = "shebang_dispatcher"

            def _finalize_script_text(
                self,
                text: str,
                job: Job,  # noqa: ARG002
                payload: Payload,  # noqa: ARG002
            ) -> str:
                return f"#!/bin/sh\n{text}"

        dispatcher = _Shebang(service, {}, identifier="mat")
        job = _script_job("echo hi")
        payload = dispatcher._resolve_job_payload(job)

        path, _ = dispatcher._materialize_script(job, payload)

        assert path is not None
        assert path.parent == private_tmpdir
        assert path.read_text() == "#!/bin/sh\necho hi"

    def test_job_without_script_writes_nothing(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        job = _bash_job(binary="true")
        payload = dispatcher._resolve_job_payload(job)

        path, context = dispatcher._materialize_script(job, payload)

        assert path is None
        assert context["script_path"] == ""
        assert list(private_tmpdir.iterdir()) == []

    def test_pass_two_failure_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """A marker this job did not sign fails resolution after mkstemp."""
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        forged = "echo \x00COURIER-DEFER:abcd:ZGlzcGF0Y2hlcg==\x00"
        job = _script_job(forged, nonce="ffff")

        with pytest.raises(CourierError, match="Failed to initialize environment"):
            dispatcher.get_execution_log(job)

        assert list(private_tmpdir.iterdir()) == []

    def test_write_failure_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        job = _script_job("echo hi")
        payload = dispatcher._resolve_job_payload(job)

        with (
            patch(
                "courier.interfaces.dispatchers.os.fchmod",
                side_effect=OSError("read-only"),
            ),
            pytest.raises(OSError, match="read-only"),
        ):
            dispatcher._materialize_script(job, payload)

        assert list(private_tmpdir.iterdir()) == []

    def test_script_is_removed_after_the_job_runs(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        logs = dispatcher.get_execution_log(_script_job('echo "$0"'))

        assert logs[0].return_code == 0
        assert logs[0].stdout.strip().startswith(str(private_tmpdir))
        assert list(private_tmpdir.iterdir()) == []

    def test_keep_file_leaves_the_script_in_place(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        """A dispatcher whose submitted work still needs the script keeps it."""

        class _Submitting(LocalDispatcher):
            name = "submitting_dispatcher"

            def _execute_job(
                self,
                job: Job,
                payload: Payload,
                env: ExecutionPayload,
            ) -> list[ExecutionLog]:
                env.keep_file = True
                return [ExecutionLog(return_code=0, stdout="", stderr="", hostname="")]

        dispatcher = _Submitting(service, {}, identifier="mat")
        dispatcher.get_execution_log(_script_job("echo hi"))

        (kept,) = private_tmpdir.iterdir()
        assert kept.read_text() == "echo hi"
        kept.unlink()


class TestEndToEndExecution:
    """Drive a real dispatcher through the consume loop with a real subprocess.

    ``_RecordingDispatcher`` overrides ``get_execution_log``, so the tests above
    never exercise hydration, rendering or process execution.  This runs
    ``LocalDispatcher`` (no overrides) end to end: consume -> hydrate -> render
    pass two -> write script -> run -> observe the file it produced.
    """

    def test_local_dispatcher_runs_the_payload_the_job_carries(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        source = tmp_path / "source.nc"
        source.write_text("payload-payload-payload")
        destination = tmp_path / "copied.nc"

        template = tmp_path / "copy.sh"
        template.write_text(f"cp {{{{ files[0].file }}}} {destination}")

        payload = BashPayload(service, {"file": template}, "payload-1")
        job = Job(
            "n",
            "job-e2e",
            {},
            files=[File(file=source).freeze()],
        )
        job.payload = payload.to_job_spec(job)

        dispatcher = LocalDispatcher(service, {}, identifier="e2e")
        _feed(dispatcher, service, job)

        assert destination.exists()
        assert destination.read_text() == "payload-payload-payload"
