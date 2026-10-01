"""Shared fixtures and helpers for plugin unit tests."""

from __future__ import annotations

import contextlib
import logging
import tempfile
from pathlib import Path

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from prometheus_client import REGISTRY

from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.execution_log import ExecutionLog
from courier.types.file import File, FrozenFile
from courier.types.job import Job

if TYPE_CHECKING:
    from collections.abc import Iterator

    from courier.interfaces.dispatchers import Dispatcher
    from courier.interfaces.payloads import Payload


@pytest.fixture(autouse=True)
def _reset_prometheus_registry():
    """Unregister per-instance plugin metrics between tests.

    Some plugins (notably ``file_system_poller_watchdog``) construct a
    Gauge in ``__init__``. Constructing two instances in the same process
    would raise ``Duplicated timeseries``. Snapshot collectors before each
    test, then drop anything new on teardown.
    """
    pre = list(REGISTRY._collector_to_names.keys())  # noqa: SLF001
    yield
    for collector in list(REGISTRY._collector_to_names.keys()):  # noqa: SLF001
        if collector not in pre:
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass


@pytest.fixture
def mock_service() -> MagicMock:
    """Service stub with the minimum attrs every plugin reads."""
    service = MagicMock()
    service._config = MagicMock()
    service._config.log_level = "DEBUG"
    service._config.loki_enabled = False
    service._config.namespace = "test-ns"
    service.config = service._config
    return service


@pytest.fixture
def service() -> MagicMock:
    """Service stub for payload and dispatcher tests, with no broker."""
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def private_tmpdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``TMPDIR`` (and ``tempfile``'s cached copy) at an empty directory."""
    directory = tmp_path / "tmpdir"
    directory.mkdir()
    monkeypatch.setenv("TMPDIR", str(directory))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return directory


@pytest.fixture
def template_config(tmp_path: Path) -> dict[str, Any]:
    """Payload config whose ``file`` is a shell template naming the job's file."""
    file = tmp_path / "demo.sh"
    file.write_text('#!/bin/sh\n\necho "hello world! file: {{ files[0].file }}"')
    return {"file": file}


@pytest.fixture
def make_file():
    """Factory that builds a File with sensible test defaults."""

    def _factory(**overrides: Any) -> File:
        defaults: dict[str, Any] = dict(
            file=Path("/tmp/x.nc"),
            hostname="testhost",
            source="goes16",
            instrument="abi",
        )
        defaults.update(overrides)
        return File(**defaults)

    return _factory


@pytest.fixture
def make_frozen_file():
    """Factory that builds a FrozenFile with sensible test defaults."""

    def _factory(**overrides: Any) -> FrozenFile:
        defaults: dict[str, Any] = dict(
            file=Path("/tmp/x.nc"),
            hostname="testhost",
            source="goes16",
            instrument="abi",
        )
        defaults.update(overrides)
        return FrozenFile(**defaults)

    return _factory


@pytest.fixture
def make_job(make_frozen_file):
    """Factory that builds a Job for dispatcher tests."""

    def _factory(files: tuple = (), **overrides: Any) -> Job:
        defaults: dict[str, Any] = dict(
            name="test-job",
            identifier="job-1",
            config={},
        )
        defaults.update(overrides)
        return Job(**defaults, files=set(files))

    return _factory


def file_job(path: Path | str = "/d/a.nc", identifier: str = "job-1") -> Job:
    """Return a job holding the one file *path*."""
    return Job("n", identifier, {}, files=[File(file=Path(path)).freeze()])


def wire_job(
    service: Any,
    payload_config: dict[str, Any],
    *,
    payload_cls: type[Payload] = BashPayload,
    payload_identifier: str = "p1",
    job: Job | None = None,
) -> Job:
    """Return *job* (default :func:`file_job`) carrying a payload, off the wire."""
    job = job if job is not None else file_job()
    payload = payload_cls(service, payload_config, payload_identifier)
    job.payload = payload.to_job_spec(job)
    return Job.from_string(str(job))


def run_locally(
    service: Any,
    payload_config: dict[str, Any],
    dispatcher_config: dict[str, Any] | None = None,
    *,
    dispatcher_cls: type[LocalDispatcher] = LocalDispatcher,
    **wire: Any,
) -> list[ExecutionLog]:
    """Run a payload through a real ``LocalDispatcher``; see :func:`wire_job`."""
    dispatcher = dispatcher_cls(service, dispatcher_config or {}, identifier="ld")
    return dispatcher.get_execution_log(wire_job(service, payload_config, **wire))


def consume(dispatcher: Dispatcher, *jobs: Job) -> None:
    """Feed *jobs* through *dispatcher*'s consume loop, then let it stop."""

    def _consume(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        for job in jobs:
            yield str(job), None
        dispatcher._stop_event.set()  # noqa: SLF001

    dispatcher.parent_service.consume.side_effect = _consume
    dispatcher.handle_incoming_jobs()


@contextlib.contextmanager
def captured_records(logger_name: str) -> Iterator[list[logging.LogRecord]]:
    """Collect the records *logger_name* emits, whatever its current setup.

    Courier loggers do not propagate, so ``caplog`` cannot see them, and their
    level depends on whichever config configured them first.
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


__all__ = [
    "ExecutionLog",
    "FrozenFile",
    "captured_records",
    "consume",
    "file_job",
    "make_file",
    "make_frozen_file",
    "make_job",
    "mock_service",
    "private_tmpdir",
    "run_locally",
    "service",
    "template_config",
    "wire_job",
]
