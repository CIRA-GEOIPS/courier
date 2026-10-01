"""Behavioural tests for the ``Dispatcher`` base class.

The consume/execute/ack loop is where one bad job could once take a whole
service down: any non-``CourierError`` escaping ``get_execution_log`` reached
``os._exit`` with the message unacked, so it recurred on every redelivery.
These tests pin the contract that replaced that:

* a job that fails while being prepared, run or published is contained and
  counted;
* a message this dispatcher cannot execute at all -- not a job, no payload,
  an unknown or incompatible payload, an invalid spec, a missing toolchain --
  is parked on the dead-letter queue rather than acknowledged and dropped;
* only a failure to park still escapes the loop.

The dispatcher hydrates the payload a job carries rather than pairing with
one, so the compatibility tests build payload specs directly.
"""

# cspell:ignore nosuch Glzc hlcg

from __future__ import annotations

import json
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


# ── construction ────────────────────────────────────────────────────────────


class TestConstruction:
    def test_identifier_is_required(self, service: MagicMock) -> None:
        """A dispatcher without an identifier has no queue to consume from."""
        with pytest.raises(ValueError, match="requires an identifier"):
            _RecordingDispatcher(service, {})

    def test_consumes_from_its_own_queue(self, service: MagicMock) -> None:
        dispatcher = _dispatcher(service, "runner-a")
        assert dispatcher.incoming_queue == job_ready_queue_for("runner-a")

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
    def test_consumed_job_is_executed_with_its_files(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = _dispatcher(service, "exec-basic")
        _feed(dispatcher, service, _job("job-1"))

        (executed,) = dispatcher.executed
        assert executed.identifier == "job-1"
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

    @pytest.mark.parametrize(
        "error",
        [PipelineError("bad job"), CourierError("own"), ValueError("stray")],
        ids=lambda error: type(error).__name__,
    )
    def test_a_job_error_fails_only_that_job(
        self,
        service: MagicMock,
        error: Exception,
    ) -> None:
        """A job's own error, a stray non-CourierError included, fails only it.

        A stray exception once reached ``os._exit`` with the message unacked,
        so the same job killed the service again on every redelivery.
        """
        dispatcher = _dispatcher(service, f"exec-{type(error).__name__}")
        calls: list[str] = []

        def _flaky(job: Job) -> list[ExecutionLog]:
            calls.append(job.identifier)
            if job.identifier == "job-1":
                raise error
            return [ExecutionLog(return_code=0, stdout="", stderr="", hostname="h")]

        before = _processed(dispatcher, "failure")
        with patch.object(dispatcher, "get_execution_log", side_effect=_flaky):
            _feed(dispatcher, service, _job("job-1"), _job("job-2"))

        assert calls == ["job-1", "job-2"]
        assert _processed(dispatcher, "failure") == before + 1
        assert len(service.emit.call_args_list) == 1, "only job-2 is published"
        service.park_message.assert_not_called()

    @pytest.mark.parametrize(
        ("path", "fault"),
        [
            pytest.param(
                "execution log",
                TransientBrokerError("connection dropped"),
                id="execution-log",
            ),
            pytest.param(
                "output files",
                kombu.exceptions.OperationalError("broker gone"),
                id="output-files",
            ),
        ],
    )
    def test_a_publish_fault_fails_only_that_job(
        self,
        service: MagicMock,
        path: str,
        fault: Exception,
    ) -> None:
        """Publishing is part of the job: a broker fault there is a failure."""
        dispatcher = _OutputFilesDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/\S+\.nc)"}]},
            identifier=f"exec-publish-{path[0]}",
        )
        target = FILE_FOUND_EXCHANGE if path == "output files" else dispatcher.queue
        faults = [fault]

        def _emit(queue: str, message: str) -> None:  # noqa: ARG001
            if queue == target and faults:
                raise faults.pop()

        service.emit.side_effect = _emit
        failures = _processed(dispatcher, "failure")
        successes = _processed(dispatcher, "success")

        _feed(dispatcher, service, _job("job-1"), _job("job-2"))  # must not raise

        assert [j.identifier for j in dispatcher.executed] == ["job-1", "job-2"]
        assert _processed(dispatcher, "failure") == failures + 1
        assert _processed(dispatcher, "success") == successes + 1

    def test_an_output_scan_error_fails_only_the_job(
        self,
        service: MagicMock,
    ) -> None:
        dispatcher = LocalDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
            identifier="exec-scan",
        )
        failures = _processed(dispatcher, "failure")
        with patch(
            "courier.interfaces.dispatchers._scan_and_emit_output_files",
            side_effect=TypeError("bad match"),
        ):
            _feed(dispatcher, service, _bash_job(binary="true"))  # must not raise

        assert _processed(dispatcher, "failure") == failures + 1

    @pytest.mark.parametrize(
        ("case", "exits"),
        [
            ("job-error", False),
            ("publish-fault", False),
            ("unexecutable", False),
            ("park-fails", True),
        ],
    )
    def test_the_process_exits_only_when_parking_fails(
        self,
        service: MagicMock,
        case: str,
        exits: bool,
    ) -> None:
        """An escaping error ends the process, leaving the message unacked."""
        dispatcher = _dispatcher(service, f"exec-exit-{case}")
        body = str(_job("job-1"))
        if case == "job-error":
            dispatcher.raise_on_execute = TypeError("job-level")
        elif case == "publish-fault":
            service.emit.side_effect = TransientBrokerError("connection dropped")
        else:
            body = "this is not json"
            if case == "park-fails":
                service.park_message.side_effect = ConnectionError("DLQ gone")
        service.consume.return_value = iter([(body, None)])

        with patch("courier.interfaces.dispatchers.os._exit") as exit_:
            dispatcher._run_handle_incoming_jobs()

        assert exit_.called is exits


# ── dedupe ──────────────────────────────────────────────────────────────────


class TestDedupe:
    @pytest.mark.parametrize("payload", [None, _spec("bash_payload", "archive")])
    def test_repeated_job_is_skipped_and_counted(
        self,
        service: MagicMock,
        payload: PayloadSpec | None,
    ) -> None:
        """A redelivery runs once; the dropped duplicate is visible in metrics."""
        identifier = f"dedupe-{'payload' if payload else 'bare'}"
        dispatcher = _dispatcher(service, identifier)
        labels = {"dispatcher_identifier": identifier}
        metric = "courier_dispatcher_dedupe_skips_total"
        before = REGISTRY.get_sample_value(metric, labels) or 0.0

        _feed(dispatcher, service, _job("dup", payload), _job("dup", payload))

        assert len(dispatcher.executed) == 1
        assert REGISTRY.get_sample_value(metric, labels) == before + 1

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

    def test_lru_is_bounded_and_evicts_the_oldest(self, service: MagicMock) -> None:
        """An unbounded set would grow without limit on a long-running node."""
        dispatcher = _dispatcher(service, "dedupe-bounded")
        for index in range(_DEDUPE_LRU_SIZE + 1):
            dispatcher._recently_seen(f"job-{index}")

        assert len(dispatcher._seen_jobs) == _DEDUPE_LRU_SIZE
        assert dispatcher._recently_seen(f"job-{_DEDUPE_LRU_SIZE}") is True
        assert dispatcher._recently_seen("job-0") is False, "oldest should be gone"


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

    @pytest.mark.parametrize(
        ("name", "cls", "interpreter"),
        [
            ("bash_payload", BashPayload, "bash"),
            ("shell_payload", ShellPayload, "sh"),
            # The most specific representation wins: python, not shell.
            ("python_payload", PythonPayload, "python"),
        ],
    )
    def test_payload_hydrates_as_its_own_class(
        self,
        service: MagicMock,
        name: str,
        cls: type[Payload],
        interpreter: str,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="celebrant")
        resolved = dispatcher._resolve_job_payload(
            _job("j", _spec(name, binary="echo"))
        )

        assert type(resolved) is cls
        assert resolved.generate_calling_method() == [interpreter, "-c"]

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

    def test_a_payload_runs_as_the_listed_class_it_derives_from(
        self,
        service: MagicMock,
    ) -> None:
        """A third-party payload lowers to a listed class, looked up once."""
        dispatcher = LocalDispatcher(service, {}, identifier="lowering")
        job = _job("j", _spec("custom_bash_payload", binary="echo"))
        with patch("courier.interfaces.dispatchers.payloads") as registry:
            registry.get_plugin.return_value = _CustomBashPayload
            first = dispatcher._resolve_job_payload(job)
            dispatcher._resolve_job_payload(job)

        assert type(first) is BashPayload
        assert first.payload_name == "custom_bash_payload"
        registry.get_plugin.assert_called_once_with("custom_bash_payload")

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

    def test_representation_names(self) -> None:
        names = "ShellPayload, BashPayload, PythonPayload"
        assert LocalDispatcher.representation_names() == names
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
                "the 'nope_payload' payload of job 'j-unknown': PluginNotFoundError",
                id="unknown-payload",
            ),
            pytest.param(
                str(_job("j-invalid", _spec("bash_payload"))),
                "the 'bash_payload' payload of job 'j-invalid': ValidationError",
                id="invalid-spec",
            ),
            pytest.param(
                str(_bash_job(binary="true", toolchain=["courier-test-no-such-tool"])),
                "'courier-test-no-such-tool' on dispatcher 'parker'",
                id="missing-toolchain",
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
                _body_with(identifier={"a": 1}),
                "'identifier' must be a string",
                id="dict-identifier",
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
                _body_with(emit_time=[1]),
                "'emit_time' is not a timestamp",
                id="list-emit-time",
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

    def test_parked_job_is_not_remembered_as_seen(self, service: MagicMock) -> None:
        """A job re-driven after the host is fixed runs, rather than being deduped.

        The loop carries on after parking, and the second delivery publishes.
        """
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


# ── materializing the job script ────────────────────────────────────────────


def _script_job(
    script: str,
    *,
    nonce: str = "",
    suffix: str = ".sh",
    **config: object,
) -> Job:
    return _job(
        "job-1",
        PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"script": "x", **config},
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

    @pytest.mark.parametrize(
        ("job", "error"),
        [
            pytest.param(
                # A malformed marker: the bare text ``dispatcher``, not a path.
                _script_job(
                    "echo \x00COURIER-DEFER:ffff:ZGlzcGF0Y2hlcg==\x00",
                    nonce="ffff",
                ),
                "marker",
                id="pass-two",
            ),
            pytest.param(
                _script_job("echo hi", suffix_args=["{{ nosuch }}"]),
                "nosuch",
                id="command-render",
            ),
        ],
    )
    def test_a_preparation_failure_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
        job: Job,
        error: str,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")

        with pytest.raises(Exception, match=error):
            dispatcher.get_execution_log(job)

        assert list(private_tmpdir.iterdir()) == []

    def test_an_execution_failure_leaves_nothing_behind(
        self,
        service: MagicMock,
        private_tmpdir: Path,
    ) -> None:
        dispatcher = LocalDispatcher(service, {}, identifier="mat")
        with (
            patch.object(
                LocalDispatcher, "_execute_job", side_effect=ValueError("bad")
            ),
            pytest.raises(ValueError, match="bad"),
        ):
            dispatcher.get_execution_log(_script_job("echo hi"))

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
