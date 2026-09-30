from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from courier.interfaces.payloads import DispatcherGroupConfig, Payload
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload, BashPayloadConfig
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import RUN_ARGV_SCRIPT, ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


@pytest.fixture
def config(tmp_path) -> dict:
    file = tmp_path / "demo.sh"
    file.write_text('#!/bin/sh\n\necho "hello world! file: {{ files[0].file }}"')
    return {"file": file}


def _job(identifier: str = "job-1") -> Job:
    return Job("n", identifier, {}, files=[File(file=Path("/d/a.nc")).freeze()])


def _base_config() -> DispatcherGroupConfig:
    return DispatcherGroupConfig.model_validate({})


class TestConstruction:
    def test_payload_construction_with_config(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = BashPayload(service, config, "dummy-payload")

        assert payload.config.file == Path(config["file"])
        assert payload.generate_calling_method() == ["bash"]

    def test_inline_binary_switches_to_bash_dash_c(
        self, service: MagicMock, config: dict
    ) -> None:
        config["binary"] = "echo"
        payload = BashPayload(service, config, "dummy-payload")

        assert payload.generate_calling_method() == ["bash", "-c"]

    def test_payload_construction_with_invalid_file_path(
        self, service: MagicMock
    ) -> None:
        with pytest.raises(ValidationError):
            BashPayload(service, {"file": "invalid_file"}, "dummy-payload")


class TestPayloadWorkflow:
    def test_get_payload_from_binary(self, service, config):
        job = _job()

        config["binary"] = "file"
        config["prefix_args"] = ["-b"]
        payload = BashPayload(service, config, "dummy-payload")
        payload.base_config = _base_config()
        result = payload.get_payload_from_job(
            ["bash", "-c", f"file -b {payload.config.file}"], job
        )

        assert len(result) > 0
        assert result[0].return_code == 0
        assert result[0].stdout
        assert "POSIX" in result[0].stdout


class TestConfigClass:
    def test_config_class_is_the_plugin_config(self) -> None:
        assert BashPayload.config_class is BashPayloadConfig

    def test_constructed_config_uses_the_config_class(
        self, service: MagicMock, config: dict
    ) -> None:
        payload = BashPayload(service, config, "dummy-payload")

        assert isinstance(payload.config, BashPayloadConfig)


def _dispatch(
    service: MagicMock,
    payload: Payload,
    dispatcher_cls: type[LocalDispatcher] = LocalDispatcher,
) -> list[ExecutionLog]:
    """Send *payload* over the wire and run it through a real dispatcher."""
    job = _job()
    job.targets = ("ld",)
    job.payload = payload.to_job_spec(job)
    wire = Job.from_string(str(job))
    return dispatcher_cls(service, {}, identifier="ld").get_execution_log(wire)


class _BashOnlyDispatcher(LocalDispatcher):
    """A dispatcher that runs Python payloads lowered to bash_payload."""

    representations: ClassVar[list[type[Payload]]] = [ShellPayload, BashPayload]


class TestInlineScriptThroughLocalDispatcher:
    def test_inline_script_runs_under_bash_with_its_arguments(
        self, service: MagicMock
    ) -> None:
        payload = BashPayload(
            service,
            {
                "script": (
                    'echo "bash=${BASH_VERSION:+yes} args=$* f={{ files[0].file }}"'
                ),
                "prefix_args": ["-e"],
                "suffix_args": ["one", "{{ job.identifier }}"],
            },
            "bash-inline",
        )

        logs = _dispatch(service, payload)

        assert logs[0].return_code == 0, logs[0].stderr
        assert "bash=yes args=one job-1 f=/d/a.nc" in logs[0].stdout

    def test_binary_mode_runs_under_bash_as_separate_arguments(
        self, service: MagicMock
    ) -> None:
        payload = BashPayload(
            service,
            {"binary": "printf", "suffix_args": ["%s|", "a b", "{{ files[0].file }}"]},
            "bash-binary",
        )
        command = payload.generate_calling_method() + payload.declare_command()

        logs = _dispatch(service, payload)

        assert command[:4] == ["bash", "-c", RUN_ARGV_SCRIPT, "bash"]
        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "a b|/d/a.nc|"


def _processed(payload_name: str, identifier: str) -> float | None:
    return REGISTRY.get_sample_value(
        "courier_payload_jobs_processed_total",
        {
            "payload_name": payload_name,
            "payload_identifier": identifier,
            "status": "success",
        },
    )


class TestLoweredPayloadMetrics:
    def test_lowered_payload_is_counted_under_its_configured_name(
        self, service: MagicMock
    ) -> None:
        """A python_payload run as bash_payload keeps its own metric label."""
        payload = PythonPayload(
            service,
            {"binary": "echo", "suffix_args": ["lowered"]},
            "lowered-metrics",
        )
        before = _processed("python_payload", "lowered-metrics") or 0.0

        logs = _dispatch(service, payload, _BashOnlyDispatcher)

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "lowered\n"
        assert _processed("python_payload", "lowered-metrics") == before + 1
        assert _processed("bash_payload", "lowered-metrics") is None
