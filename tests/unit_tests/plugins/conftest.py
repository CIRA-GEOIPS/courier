"""Shared fixtures for plugin unit tests."""

from __future__ import annotations

from pathlib import Path

from typing import Any
from unittest.mock import MagicMock

import pytest

from prometheus_client import REGISTRY

from courier.types.execution_log import ExecutionLog
from courier.types.file import File, FrozenFile
from courier.types.job import Job
from courier.utils.bash_executor import BashExecResult


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



@pytest.fixture
def fake_bash_exec_result():
    """Build BashExecResult for bash dispatcher tests."""
    def _factory(
        return_code: int = 0,
        stdout: str = "ok",
        stderr: str = "",
        log_file_path: str | None = None,
    ) -> BashExecResult:
        return BashExecResult(
            return_code=return_code,
            stdout=stdout,
            stderr=stderr,
            log_file_path=log_file_path,
        )
    return _factory


__all__ = [
    "ExecutionLog",
    "FrozenFile",
    "make_file",
    "make_frozen_file",
    "make_job",
    "mock_service",
]
