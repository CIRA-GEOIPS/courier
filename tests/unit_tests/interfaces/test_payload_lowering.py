"""A payload lowered to an ancestor still runs with its own interpreter.

A dispatcher runs a payload as the most specific class in its hierarchy that
it lists in ``representations``.  When that is an ancestor, the payload is
still hydrated as its own class and its command is passed through
:meth:`Payload.render_script`, which hands it to the ancestor's
``wrap_command``: a Python script lowered to bash is run by ``bash`` running
``python <script>``, never by ``bash <script>``.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import patch

import pytest

from courier.errors import UnexecutableJobError
from courier.interfaces.payloads import Payload
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import SUBPROCESS_WRAPPER, PythonPayload
from courier.plugins.payloads.shell_payload import RUN_ARGV_SCRIPT, ShellPayload
from tests._helpers import run_locally, wire_job

if TYPE_CHECKING:
    from unittest.mock import MagicMock


class _BashOnlyDispatcher(LocalDispatcher):
    name = "bash_only_dispatcher"
    representations: ClassVar[list[type[Payload]]] = [BashPayload]


class _ShellOnlyDispatcher(LocalDispatcher):
    name = "shell_only_dispatcher"
    representations: ClassVar[list[type[Payload]]] = [ShellPayload]


class _PythonOnlyDispatcher(LocalDispatcher):
    name = "python_only_dispatcher"
    representations: ClassVar[list[type[Payload]]] = [PythonPayload]


class _CustomPythonPayload(PythonPayload):
    """A third-party payload that shipped dispatchers run as ``PythonPayload``."""

    name = "custom_python_payload"


def _python(**config: object) -> dict[str, object]:
    return {"default_binary": sys.executable, **config}


class TestRenderScript:
    def test_a_payload_run_as_itself_keeps_its_command(
        self,
        service: MagicMock,
    ) -> None:
        payload = PythonPayload(service, _python(script="print(1)"), "p")

        assert not payload.lowered
        assert payload.render_script(["python", "x.py"]) == ["python", "x.py"]

    def test_lowered_to_bash_the_command_is_run_by_bash(
        self,
        service: MagicMock,
    ) -> None:
        job = wire_job(service, _python(script="print(1)"), payload_cls=PythonPayload)
        payload = PythonPayload.from_job_spec(
            job.payload,
            service,
            representation=BashPayload,
        )

        assert payload.lowered
        assert payload.render_script([sys.executable, "/s.py"]) == [
            "bash",
            "-c",
            RUN_ARGV_SCRIPT,
            "bash",
            sys.executable,
            "/s.py",
        ]

    def test_lowered_to_python_the_command_is_run_from_python(
        self,
        service: MagicMock,
    ) -> None:
        job = wire_job(service, _python(script="x"), payload_cls=_CustomPythonPayload)
        payload = _CustomPythonPayload.from_job_spec(
            job.payload,
            service,
            representation=PythonPayload,
        )

        assert payload.render_script(["tool", "a b"]) == [
            "python",
            "-c",
            SUBPROCESS_WRAPPER,
            "tool",
            "a b",
        ]

    def test_a_representation_outside_the_hierarchy_is_refused(
        self,
        service: MagicMock,
    ) -> None:
        job = wire_job(service, {"script": "echo hi"})

        with pytest.raises(TypeError, match="not in its representation hierarchy"):
            BashPayload.from_job_spec(
                job.payload,
                service,
                representation=PythonPayload,
            )

    def test_the_base_class_cannot_wrap_a_command(self) -> None:
        with pytest.raises(UnexecutableJobError, match="cannot run the command"):
            Payload.wrap_command(["true"])


class TestLoweredExecution:
    def test_python_lowered_to_bash_runs_as_python(
        self,
        service: MagicMock,
    ) -> None:
        (log,) = run_locally(
            service,
            _python(script="import sys; print('python', sys.argv[1:])"),
            dispatcher_cls=_BashOnlyDispatcher,
            payload_cls=PythonPayload,
        )

        assert log.return_code == 0, log.stderr
        assert log.stdout == "python []\n"

    def test_lowered_arguments_stay_separate(self, service: MagicMock) -> None:
        (log,) = run_locally(
            service,
            _python(
                script="import sys; print(sys.argv[1:])",
                suffix_args=["a b", "$HOME", "{{ job.identifier }}"],
            ),
            dispatcher_cls=_ShellOnlyDispatcher,
            payload_cls=PythonPayload,
        )

        assert log.return_code == 0, log.stderr
        assert log.stdout == "['a b', '$HOME', 'job-1']\n"

    def test_bash_lowered_to_sh_runs_as_bash(self, service: MagicMock) -> None:
        (log,) = run_locally(
            service,
            {"script": 'if [[ -n "$BASH_VERSION" ]]; then echo bash; fi'},
            dispatcher_cls=_ShellOnlyDispatcher,
        )

        assert log.return_code == 0, log.stderr
        assert log.stdout == "bash\n"

    def test_a_third_party_payload_runs_with_its_own_command(
        self,
        service: MagicMock,
    ) -> None:
        with patch("courier.interfaces.dispatchers.payloads") as registry:
            registry.get_plugin.return_value = _CustomPythonPayload
            (log,) = run_locally(
                service,
                _python(script="print('custom')"),
                dispatcher_cls=_PythonOnlyDispatcher,
                payload_cls=_CustomPythonPayload,
            )

        assert log.return_code == 0, log.stderr
        assert log.stdout == "custom\n"

    def test_the_toolchain_probe_is_lowered_too(self, service: MagicMock) -> None:
        """The probe is Python code: lowered, it must still reach Python."""
        dispatcher = _BashOnlyDispatcher(service, {}, identifier="ld")
        job = wire_job(
            service,
            _python(script="print(1)", toolchain=["sh"]),
            payload_cls=PythonPayload,
        )

        with patch.object(
            PythonPayload,
            "get_payload_from_job",
            autospec=True,
            side_effect=PythonPayload.get_payload_from_job,
        ) as run:
            payload = dispatcher._resolve_job_payload(job)

        assert payload.representation is BashPayload
        probe = run.call_args_list[0].args[1]
        assert probe[:4] == ["bash", "-c", RUN_ARGV_SCRIPT, "bash"]
        assert probe[4:6] == [sys.executable, "-c"]
