from typing import ClassVar
from unittest.mock import MagicMock

from prometheus_client import REGISTRY

from courier.interfaces.payloads import Payload
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import RUN_ARGV_SCRIPT, ShellPayload
from tests.unit_tests.plugins.conftest import run_locally


class TestConstruction:
    def test_inline_binary_switches_to_bash_dash_c(
        self, service: MagicMock, template_config: dict
    ) -> None:
        template_config["binary"] = "echo"
        payload = BashPayload(service, template_config, "dummy-payload")

        assert payload.generate_calling_method() == ["bash", "-c"]


class _BashOnlyDispatcher(LocalDispatcher):
    """A dispatcher that runs Python payloads lowered to bash_payload."""

    representations: ClassVar[list[type[Payload]]] = [ShellPayload, BashPayload]


class TestInlineScriptThroughLocalDispatcher:
    def test_inline_script_runs_under_bash_with_its_arguments(
        self, service: MagicMock
    ) -> None:
        logs = run_locally(
            service,
            {
                "script": (
                    'echo "bash=${BASH_VERSION:+yes} args=$* f={{ files[0].file }}"'
                ),
                "prefix_args": ["-e"],
                "suffix_args": ["one", "{{ job.identifier }}"],
            },
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert "bash=yes args=one job-1 f=/d/a.nc" in logs[0].stdout

    def test_binary_mode_runs_under_bash_as_separate_arguments(
        self, service: MagicMock
    ) -> None:
        config = {
            "binary": "printf",
            "suffix_args": ["%s|", "a b", "{{ files[0].file }}"],
        }
        payload = BashPayload(service, config, "bash-binary")
        command = payload.generate_calling_method() + payload.declare_command()

        logs = run_locally(service, config)

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
        before = _processed("python_payload", "lowered-metrics") or 0.0

        logs = run_locally(
            service,
            {"binary": "echo", "suffix_args": ["lowered"]},
            payload_cls=PythonPayload,
            payload_identifier="lowered-metrics",
            dispatcher_cls=_BashOnlyDispatcher,
        )

        assert logs[0].return_code == 0, logs[0].stderr
        assert logs[0].stdout == "lowered\n"
        assert _processed("python_payload", "lowered-metrics") == before + 1
        assert _processed("bash_payload", "lowered-metrics") is None
