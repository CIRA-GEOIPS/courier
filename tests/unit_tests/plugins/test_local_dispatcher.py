"""Behavioural tests for the local dispatcher and its payload execution."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from courier.constants import FILE_FOUND_EXCHANGE
from courier.metrics import COURIER_CUSTOM_GAUGE
from courier.plugins.dispatchers.local_dispatcher import (
    LocalDispatcher,
    _ingest_courier_metrics,
)
from courier.plugins.payloads.bash_payload import BashPayload
from courier.types.file import File
from courier.types.job import Job


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _job_with_payload(service: MagicMock, payload: BashPayload, **kwargs) -> Job:
    job = Job("n", kwargs.get("identifier", "job-1"), {})
    job.targets = ("ld",)
    job.payload = payload.to_job_spec(job)
    return Job.from_string(str(job))


class TestExecution:
    def test_binary_payload_executes(self, service: MagicMock, tmp_path: Path) -> None:
        source = tmp_path / "in.txt"
        source.write_text("payload")
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        payload = BashPayload(
            service,
            {
                "binary": "cp",
                "suffix_args": ["{{ files[0].file }}", str(output_dir)],
            },
            "p1",
        )
        job = Job("n", "job-1", {}, files=[File(file=source).freeze()])
        job.targets = ("ld",)
        job.payload = payload.to_job_spec(job)
        wire = Job.from_string(str(job))

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0
        assert (output_dir / "in.txt").exists()

    def test_deferred_dispatcher_values_resolve(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text('echo "disp={{ dispatcher.identifier }}"')
        payload = BashPayload(service, {"file": template}, "p1")
        wire = _job_with_payload(service, payload)

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(wire)

        assert logs[0].return_code == 0
        assert "disp=ld" in logs[0].stdout

    def test_untrusted_data_is_literal(self, service: MagicMock, tmp_path: Path) -> None:
        template = tmp_path / "t.sh"
        template.write_text("echo {{ files[0].file }}")
        payload = BashPayload(service, {"file": template}, "p1")
        job = Job(
            "n",
            "job-1",
            {},
            files=[File(file=Path("/data/{{ 6*7 }}.nc")).freeze()],
        )
        job.targets = ("ld",)
        job.payload = payload.to_job_spec(job)

        dispatcher = LocalDispatcher(service, {}, identifier="ld")
        logs = dispatcher.get_execution_log(Job.from_string(str(job)))

        assert "{{ 6*7 }}" in logs[0].stdout
        assert "42" not in logs[0].stdout


class TestCourierMetric:
    def test_wellformed_metric_is_recorded(self) -> None:
        _ingest_courier_metrics("COURIER_METRIC: files_written 42", "ld")

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="files_written",
        )
        assert gauge._value.get() == 42.0  # noqa: SLF001

    def test_malformed_metric_does_not_raise(self) -> None:
        _ingest_courier_metrics("COURIER_METRIC: malformed_metric 1.2.3", "ld")

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="malformed_metric",
        )
        assert gauge._value.get() == 0.0  # noqa: SLF001

    def test_execution_ingests_metrics(self, service: MagicMock, tmp_path: Path) -> None:
        template = tmp_path / "t.sh"
        template.write_text("echo 'COURIER_METRIC: jobs_done 7'")
        payload = BashPayload(service, {"file": template}, "p1")
        wire = _job_with_payload(service, payload)

        LocalDispatcher(service, {}, identifier="ld").get_execution_log(wire)

        gauge = COURIER_CUSTOM_GAUGE.labels(
            dispatcher_identifier="ld",
            metric_name="jobs_done",
        )
        assert gauge._value.get() == 7.0  # noqa: SLF001


class TestOutputFiles:
    def test_output_file_is_re_emitted(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        produced = tmp_path / "product.nc"
        template = tmp_path / "t.sh"
        template.write_text(f'echo "{produced}"')
        payload = BashPayload(service, {"file": template}, "p1")
        wire = _job_with_payload(service, payload)

        dispatcher = LocalDispatcher(
            service,
            {"output_files": [{"pattern": r"(?P<file>/.*\.nc)"}]},
            identifier="ld",
        )
        dispatcher.get_execution_log(wire)

        emitted = [
            call
            for call in service.emit.call_args_list
            if call.kwargs.get("queue") == FILE_FOUND_EXCHANGE
        ]
        assert emitted, "output file was not re-emitted"
        file_obj = File.from_string(emitted[0].kwargs["message"])
        assert str(file_obj.file) == str(produced)

    def test_no_output_files_means_no_scan(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text(f'echo "{tmp_path / "x.nc"}"')
        payload = BashPayload(service, {"file": template}, "p1")
        wire = _job_with_payload(service, payload)

        LocalDispatcher(service, {}, identifier="ld").get_execution_log(wire)

        assert all(
            call.kwargs.get("queue") != FILE_FOUND_EXCHANGE
            for call in service.emit.call_args_list
        )
