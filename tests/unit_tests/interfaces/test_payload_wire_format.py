"""The job message carries a payload's template once: rendered.

``Payload.to_job_spec`` used to serialize the whole config, so an inline
``script`` was sent twice -- raw in ``PayloadSpec.config`` and rendered in
``PayloadSpec.script``.  Now the rendered script is the only copy on the wire,
and a dispatcher hydrating the spec puts it back into ``config.script`` for
the code that inspects it.  These tests drive every in-tree job builder's emit
path and a real dispatcher, so the whole round trip is covered.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest

from courier.errors import CourierError
from courier.interfaces.job_builders import JobBuilder, job_builders
from courier.interfaces.payloads import _config_from_job_spec
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcher
from courier.plugins.dispatchers.slurm_dispatcher import SlurmDispatcher
from courier.plugins.payloads.bash_payload import BashPayload
from courier.plugins.payloads.python_payload import PythonPayload
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.file import File
from courier.types.job import Job
from courier.types.payload import PayloadSpec
from tests._helpers import bind_payload

if TYPE_CHECKING:
    from courier.interfaces.payloads import Payload

#: Text that occurs in every template below and nowhere else in a job.
UNIQUE = "wire-format-7f3a9c"
DATA_FILE = "/data/in-a.nc"


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    svc.target_resolver = None
    return svc


#: ``(payload plugin, template, template file suffix)``.  Each template prints
#: UNIQUE, the job's file and the dispatcher's identifier (a pass-two value).
_TEMPLATES: dict[str, tuple[str, str, str]] = {
    "bash_payload": (
        "bash_payload",
        f'echo "{UNIQUE} {{{{ files[0].file }}}} {{{{ dispatcher.identifier }}}}"',
        ".sh",
    ),
    "shell_payload": (
        "shell_payload",
        f'echo "{UNIQUE} {{{{ files[0].file }}}} {{{{ dispatcher.identifier }}}}"',
        ".sh",
    ),
    "python_payload": (
        "python_payload",
        f'print("{UNIQUE} {{{{ files[0].file }}}} {{{{ dispatcher.identifier }}}}")',
        ".py",
    ),
}
_EXPECTED_STDOUT = f"{UNIQUE} {DATA_FILE} ld"


def _payload_config(
    kind: str,
    source: str,
    tmp_path: Path,
) -> tuple[str, dict[str, Any], str]:
    """Return ``(plugin name, payload config, raw template)`` for a case."""
    name, template, suffix = _TEMPLATES[kind]
    if source == "inline":
        return name, {"script": template}, template
    path = tmp_path / f"template{suffix}"
    path.write_text(template)
    return name, {"file": str(path)}, template


#: Every in-tree job builder, with the settings besides ``targets`` and
#: ``payload`` that it needs to emit one job per file.
_BUILDER_SETTINGS: dict[str, dict[str, Any]] = {
    "DummyJobBuilder": {},
    "file_count_builder": {"files_per_job": 1},
    "filter_and_group": {"files_per_job": 1},
    "metadata_router": {"routes": [{"name": "all", "files_per_job": 1}]},
}


def test_every_in_tree_builder_is_covered() -> None:
    """Guard the guard: a builder missing here would go untested."""
    assert set(_BUILDER_SETTINGS) <= set(job_builders.names())


def _builder(
    service: MagicMock,
    name: str,
    config: dict[str, Any],
    builder: str = "file_count_builder",
) -> JobBuilder:
    """Build a *builder* whose payload block is *name* / *config*, and bind it."""
    builder_cls = cast("type[JobBuilder]", job_builders.get_plugin(builder))
    instance = builder_cls(
        service,
        {
            **_BUILDER_SETTINGS[builder],
            "targets": ["ld"],
            "payload": {"p1": {"kind": "payload", "name": name, "config": config}},
        },
        identifier="b1",
    )
    bind_payload(instance)
    return instance


def _published_job(service: MagicMock, builder: JobBuilder) -> str:
    """Feed one file through *builder*; return the one message it published."""
    builder._dispatch_file(File(file=Path(DATA_FILE)).freeze())
    (call,) = service.emit.call_args_list
    message: str = call.kwargs["message"]
    service.emit.reset_mock()
    return message


_CASES = [
    pytest.param(kind, source, id=f"{kind}-{source}")
    for kind in _TEMPLATES
    for source in ("inline", "file")
]


class TestTheTemplateTravelsOnce:
    @pytest.mark.parametrize("builder", list(_BUILDER_SETTINGS))
    @pytest.mark.parametrize(("kind", "source"), _CASES)
    def test_message_holds_the_rendered_script_only(
        self,
        service: MagicMock,
        tmp_path: Path,
        kind: str,
        source: str,
        builder: str,
    ) -> None:
        name, config, template = _payload_config(kind, source, tmp_path)

        message = _published_job(service, _builder(service, name, config, builder))

        assert message.count(UNIQUE) == 1
        assert json.dumps(template)[1:-1] not in message, "raw template was sent"
        assert "files[0].file" not in message
        spec = Job.from_string(message).payload
        assert spec is not None
        assert "script" not in spec.config
        assert spec.script is not None
        assert UNIQUE in spec.script
        assert DATA_FILE in spec.script

    def test_a_file_payload_still_sends_its_path(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        name, config, _ = _payload_config("bash_payload", "file", tmp_path)

        message = _published_job(service, _builder(service, name, config))

        spec = Job.from_string(message).payload
        assert spec is not None
        assert spec.config["file"] == config["file"]

    @pytest.mark.parametrize(("kind", "source"), _CASES)
    def test_round_trip_runs_on_a_local_dispatcher(
        self,
        service: MagicMock,
        tmp_path: Path,
        kind: str,
        source: str,
    ) -> None:
        name, config, _ = _payload_config(kind, source, tmp_path)
        if kind == "python_payload":
            config["default_binary"] = sys.executable
        message = _published_job(service, _builder(service, name, config))
        if source == "file":
            # The template need not exist where the dispatcher runs.
            Path(config["file"]).unlink()

        (log,) = LocalDispatcher(service, {}, identifier="ld").get_execution_log(
            Job.from_string(message),
        )

        assert log.return_code == 0, log.stderr
        assert (log.stdout or "").strip() == _EXPECTED_STDOUT

    @pytest.mark.parametrize(("kind", "source"), _CASES)
    def test_an_older_builders_message_still_runs(
        self,
        service: MagicMock,
        tmp_path: Path,
        kind: str,
        source: str,
    ) -> None:
        """A builder that still sends ``config.script`` sends the raw template.

        (``null`` for a file payload.)  The dispatcher runs the rendered copy:
        the raw one is never rendered or run, even though it still names
        builder-side values.
        """
        name, config, template = _payload_config(kind, source, tmp_path)
        if kind == "python_payload":
            config["default_binary"] = sys.executable
        body = json.loads(_published_job(service, _builder(service, name, config)))
        body["payload"]["config"]["script"] = template if source == "inline" else None
        older = json.dumps(body)
        assert older.count(UNIQUE) == (2 if source == "inline" else 1)

        (log,) = LocalDispatcher(service, {}, identifier="ld").get_execution_log(
            Job.from_string(older),
        )

        assert log.return_code == 0, log.stderr
        assert (log.stdout or "").strip() == _EXPECTED_STDOUT


# ── hydration ───────────────────────────────────────────────────────────────


def _spec(config: dict[str, Any], script: str | None) -> PayloadSpec:
    return PayloadSpec(
        name="bash_payload",
        identifier="p1",
        config=config,
        script=script,
    )


class TestHydration:
    def test_the_rendered_script_satisfies_the_source_requirement(self) -> None:
        config = _config_from_job_spec(_spec({}, "echo rendered"))

        assert config.script == "echo rendered"

    def test_a_file_spec_is_given_the_rendered_script_too(self) -> None:
        config = _config_from_job_spec(
            _spec({"file": "/nowhere/template.sh"}, "echo rendered"),
        )

        assert config.file == Path("/nowhere/template.sh")
        assert config.script == "echo rendered"

    def test_an_older_builders_spec_still_hydrates(self) -> None:
        """A builder that still sends ``config.script`` sends the raw template.

        The rendered copy is what runs, so it replaces the raw one.
        """
        config = _config_from_job_spec(
            _spec({"script": "echo {{ files[0].file }}"}, "echo /data/a.nc"),
        )

        assert config.script == "echo /data/a.nc"

    def test_a_spec_with_no_script_or_source_is_invalid(self) -> None:
        with pytest.raises(ValueError, match="'file', 'script', or 'binary'"):
            _config_from_job_spec(_spec({"script": "echo raw"}, None))

    def test_a_binary_spec_needs_no_script(self) -> None:
        config = _config_from_job_spec(_spec({"binary": "true"}, None))

        assert config.script is None
        assert config.binary == "true"

    def test_a_hydrated_payload_refuses_to_render_again(
        self,
        service: MagicMock,
    ) -> None:
        """Its ``config.script`` is rendered text: rendering it would run data."""
        hydrated = BashPayload.from_job_spec(
            _spec({}, "echo {{ 7 * 6 }} came from job data"),
            service,
        )

        with pytest.raises(CourierError, match="already rendered"):
            hydrated.to_job_spec(Job("n", "job-1", {}))


# ── dispatcher logic that inspects config.script ────────────────────────────


def _wire_job(payload: Payload) -> Job:
    """Return a job carrying *payload*'s spec, as a dispatcher receives it."""
    job = Job("n", "job-1", {}, files=[File(file=Path(DATA_FILE)).freeze()])
    job.targets = ("sd",)
    job.payload = payload.to_job_spec(job)
    return Job.from_string(str(job))


class TestDispatcherSideDecisions:
    @pytest.mark.parametrize(
        ("payload_cls", "config", "python_source"),
        [
            (PythonPayload, {"script": "print(1)"}, True),
            (PythonPayload, {"file": "t.py"}, True),
            (PythonPayload, {"file": "t.sh"}, False),
            (PythonPayload, {"script": "x", "binary": "echo"}, False),
        ],
    )
    def test_python_inline_detection_survives_the_wire(
        self,
        service: MagicMock,
        tmp_path: Path,
        payload_cls: type[PythonPayload],
        config: dict[str, Any],
        *,
        python_source: bool,
    ) -> None:
        if "file" in config:
            template = tmp_path / config["file"]
            template.write_text("print(1)\n")
            config = {**config, "file": str(template)}
        job = _wire_job(payload_cls(service, config, "p1"))
        assert job.payload is not None

        hydrated = PythonPayload.from_job_spec(job.payload, service)

        assert hydrated._runs_python_source() is python_source
        assert ("-c" in hydrated.generate_calling_method()) is not python_source

    @pytest.mark.parametrize(
        ("payload_cls", "config", "batch"),
        [
            (BashPayload, {"script": "echo hi"}, True),
            (ShellPayload, {"script": "echo hi"}, True),
            (BashPayload, {"script": "echo hi", "prefix_args": ["-x"]}, False),
            (BashPayload, {"script": "echo hi", "binary": "echo"}, False),
            (PythonPayload, {"script": "print(1)"}, False),
        ],
    )
    def test_slurm_batch_or_wrap_survives_the_wire(
        self,
        service: MagicMock,
        tmp_path: Path,
        payload_cls: type[Payload],
        config: dict[str, Any],
        *,
        batch: bool,
    ) -> None:
        dispatcher = SlurmDispatcher(
            service,
            {"slurm_output_dir": str(tmp_path / "out")},
            identifier="sd",
        )
        job = _wire_job(payload_cls(service, config, "p1"))
        payload = dispatcher._resolve_job_payload(job)

        env = dispatcher.initialize_environment(job, payload)

        assert ("--wrap" not in env.command) is batch
        assert env.file is not None
        assert env.file.read_text().startswith("#!") is batch
