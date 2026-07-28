"""Behavioural tests for the ``DataMonitorBasePlugin`` base class.

Data monitors are the pipeline's producers: whatever ``find_file`` yields is
enriched and published to the file-found exchange. The enrichment and emission
path had no dedicated coverage, and its failure modes are quiet — a file that
is never emitted looks identical to a file that never arrived.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from courier.constants import FILE_FOUND_EXCHANGE, PluginRunState
from courier.errors import PipelineError
from courier.interfaces.module_based.data_monitors import DataMonitorBasePlugin
from courier.types.file import File


class _ScriptedMonitor(DataMonitorBasePlugin):
    """Monitor that yields a fixed list of files, then stops."""

    name = "scripted_monitor"
    version = "test"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.to_yield: list[File] = []

    def find_file(self) -> Generator[File, None, None]:
        yield from self.to_yield


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    return svc


def _monitor(service: MagicMock, identifier: str = "dm-1") -> _ScriptedMonitor:
    return _ScriptedMonitor(service, {}, identifier=identifier)


def _emitted_files(service: MagicMock) -> list[File]:
    return [
        File.from_string(call.kwargs["message"])
        for call in service.emit.call_args_list
    ]


class TestEmission:
    """Everything ``find_file`` yields must reach the exchange."""

    def test_yielded_file_is_published(self, service: MagicMock) -> None:
        monitor = _monitor(service)
        monitor.to_yield = [File(file=Path("/data/a.nc"), hostname="h")]

        monitor.find_and_emit_files()

        assert service.emit.call_args.kwargs["queue"] == FILE_FOUND_EXCHANGE
        assert [str(f.file) for f in _emitted_files(service)] == ["/data/a.nc"]

    def test_every_file_is_published(self, service: MagicMock) -> None:
        """A partial drain would silently lose files mid-scan."""
        monitor = _monitor(service)
        monitor.to_yield = [File(file=Path(f"/data/{n}.nc")) for n in "abc"]

        monitor.find_and_emit_files()

        assert [str(f.file) for f in _emitted_files(service)] == [
            "/data/a.nc",
            "/data/b.nc",
            "/data/c.nc",
        ]

    def test_file_metadata_survives_serialisation(
        self,
        service: MagicMock,
    ) -> None:
        """Builders filter on these fields; losing them breaks routing."""
        monitor = _monitor(service)
        monitor.to_yield = [
            File(
                file=Path("/data/a.nc"),
                hostname="host-1",
                source="goes18",
                instrument="abi",
                domain="Full-Disk",
                metadata={"level": "l1b"},
            ),
        ]

        monitor.find_and_emit_files()

        (emitted,) = _emitted_files(service)
        assert emitted.hostname == "host-1"
        assert emitted.source == "goes18"
        assert emitted.instrument == "abi"
        assert emitted.domain == "Full-Disk"
        assert emitted.metadata == {"level": "l1b"}

    def test_no_files_publishes_nothing(self, service: MagicMock) -> None:
        monitor = _monitor(service)
        monitor.find_and_emit_files()
        service.emit.assert_not_called()

    def test_pipeline_error_on_one_file_does_not_stop_the_scan(
        self,
        service: MagicMock,
    ) -> None:
        """One unparseable file must not abort the rest of the directory."""
        monitor = _monitor(service)
        monitor.to_yield = [
            File(file=Path("/data/bad.nc")),
            File(file=Path("/data/good.nc")),
        ]
        service.emit.side_effect = [PipelineError("bad metadata"), None]

        monitor.find_and_emit_files()  # must not raise

        assert service.emit.call_count == 2


class TestMetrics:
    """Counters are read back from the registry, as Prometheus would."""

    def test_success_is_counted(self, service: MagicMock) -> None:
        monitor = _monitor(service, identifier="dm-success")
        monitor.to_yield = [File(file=Path("/data/a.nc"))]
        labels = {
            "monitor_name": monitor.name,
            "monitor_identifier": "dm-success",
            "status": "success",
        }
        before = REGISTRY.get_sample_value(
            "courier_data_monitor_files_processed_total", labels,
        ) or 0.0

        monitor.find_and_emit_files()

        after = REGISTRY.get_sample_value(
            "courier_data_monitor_files_processed_total", labels,
        )
        assert after == before + 1

    def test_failure_is_counted_separately(self, service: MagicMock) -> None:
        monitor = _monitor(service, identifier="dm-failure")
        monitor.to_yield = [File(file=Path("/data/a.nc"))]
        service.emit.side_effect = PipelineError("nope")
        labels = {
            "monitor_name": monitor.name,
            "monitor_identifier": "dm-failure",
            "status": "failure",
        }
        before = REGISTRY.get_sample_value(
            "courier_data_monitor_files_processed_total", labels,
        ) or 0.0

        monitor.find_and_emit_files()

        after = REGISTRY.get_sample_value(
            "courier_data_monitor_files_processed_total", labels,
        )
        assert after == before + 1

    def test_freshness_gauge_advances(self, service: MagicMock) -> None:
        """Dashboards alert on this gauge going stale."""
        monitor = _monitor(service, identifier="dm-fresh")
        monitor.to_yield = [File(file=Path("/data/a.nc"))]
        labels = {"plugin_name": monitor.name, "monitor_identifier": "dm-fresh"}

        monitor.find_and_emit_files()

        assert REGISTRY.get_sample_value(
            "courier_data_monitor_last_processed_timestamp_seconds", labels,
        ) > 0


class TestLifecycle:
    def test_start_then_stop_leaves_no_live_thread(
        self,
        service: MagicMock,
    ) -> None:
        """A monitor thread that outlives stop() blocks interpreter exit."""
        monitor = _monitor(service)

        monitor.start()
        assert monitor.is_healthy() is True
        monitor.stop()

        assert monitor._stop_event.is_set()
        assert monitor._state is PluginRunState.STOPPED
        assert not (monitor._main_thread and monitor._main_thread.is_alive())

    def test_start_is_idempotent(self, service: MagicMock) -> None:
        monitor = _monitor(service)
        monitor.start()
        first_thread = monitor._main_thread

        monitor.start()

        assert monitor._main_thread is first_thread
        monitor.stop()

    def test_identifier_defaults_to_the_plugin_name(
        self,
        service: MagicMock,
    ) -> None:
        """Metrics still get a usable label when no identifier is configured."""
        monitor = _ScriptedMonitor(service, {})
        assert monitor.identifier == monitor.name
