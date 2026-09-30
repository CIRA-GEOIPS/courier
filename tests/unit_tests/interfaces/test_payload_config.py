"""Payload construction, config hygiene and script writing.

Covers the builder-side contract of :class:`~courier.interfaces.payloads.Payload`
that does not involve deferred values: the template is read and compiled once
at construction, configs are validated by ``config_class`` on both
construction paths, removed or misplaced keys fail with advice, and scripts
are written without following symlinks or ignoring ``TMPDIR``.
"""

# cspell:ignore secnds scirpt

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError, create_model

from courier.interfaces import discovery
from courier.interfaces.dispatchers import Dispatcher, dispatchers
from courier.interfaces.payloads import (
    REMOVED_DISPATCHER_KEYS,
    DispatcherGroupConfig,
    Payload,
    PayloadConfig,
    payloads,
)
from courier.metrics import PAYLOAD_JOBS_PROCESSED
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcherConfig
from courier.plugins.dispatchers.slurm_dispatcher import SlurmDispatcherConfig
from courier.plugins.payloads.bash_payload import BashPayload, BashPayloadConfig
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job
from courier.types.payload import PayloadSpec

if TYPE_CHECKING:
    from collections.abc import Callable

_REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def service() -> MagicMock:
    svc = MagicMock()
    svc.config = MagicMock(log_level="DEBUG", loki_enabled=False, namespace="ns")
    svc._broker_manager._connection = None
    return svc


def _job() -> Job:
    return Job("n", "job-1", {}, files=[File(file=Path("/data/a.nc"))])


# ── the template is read and compiled once, at construction ─────────────────


class TestTemplateAtConstruction:
    def test_syntax_error_in_a_template_file_fails_construction(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "broken.sh"
        template.write_text("echo ok\necho {{ files[0].file \n")

        with pytest.raises(ValueError, match=r"broken\.sh.*line 2") as info:
            BashPayload(service, {"file": template}, "p")

        assert "invalid Jinja template" in str(info.value)

    def test_syntax_error_in_an_inline_script_fails_construction(
        self,
        service: MagicMock,
    ) -> None:
        with pytest.raises(ValueError, match=r"inline script, line 1"):
            BashPayload(service, {"script": "{% if %}"}, "p")

    def test_unknown_filter_fails_construction(self, service: MagicMock) -> None:
        with pytest.raises(ValueError, match="inline script"):
            BashPayload(service, {"script": "{{ files | no_such_filter }}"}, "p")

    def test_unreadable_template_fails_construction(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        # A directory exists (passing the config check) but cannot be read.
        with pytest.raises(ValueError, match="cannot read template file"):
            BashPayload(service, {"file": tmp_path}, "p")

    def test_undecodable_template_fails_construction(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "latin1.sh"
        template.write_bytes(b"echo \xff\xfe\n")

        with pytest.raises(ValueError, match=r"cannot read template file.*latin1"):
            BashPayload(service, {"file": template}, "p")

    @pytest.mark.parametrize(
        ("config", "origin"),
        [
            ({"binary": "echo {{", "script": "x"}, "binary"),
            ({"script": "x", "prefix_args": ["ok", "{% bad"]}, r"prefix_args\[1\]"),
            ({"script": "x", "suffix_args": ["{{ }"]}, r"suffix_args\[0\]"),
        ],
    )
    def test_syntax_error_in_an_argument_template_fails_construction(
        self,
        service: MagicMock,
        config: dict[str, Any],
        origin: str,
    ) -> None:
        with pytest.raises(ValueError, match=origin):
            BashPayload(service, config, "p")

    def test_template_file_is_read_once_not_on_every_emit(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        template = tmp_path / "t.sh"
        template.write_text("echo original {{ files[0].file }}")
        payload = BashPayload(service, {"file": template}, "p")

        template.write_text("echo edited")
        first = payload.to_job_spec(_job())
        template.unlink()
        second = payload.to_job_spec(_job())

        assert first.script == "echo original /data/a.nc"
        assert second.script == first.script
        assert first.suffix == ".sh"

    def test_hydration_never_reads_the_template(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        spec = PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"file": str(tmp_path / "absent.sh")},
            script="echo {{ not jinja on the dispatcher",
        )

        payload = BashPayload.from_job_spec(spec, service)

        assert payload.config.file == tmp_path / "absent.sh"


# ── config_class, base_config and payload_name on both construction paths ───


class _ProductPayloadConfig(BashPayloadConfig):
    product: str = "default-product"


class _ProductPayload(BashPayload):
    name: ClassVar[str] = "product_payload"
    config_class: ClassVar[type[PayloadConfig]] = _ProductPayloadConfig


class TestConstructionPaths:
    def test_init_validates_with_the_config_class(self, service: MagicMock) -> None:
        payload = _ProductPayload(service, {"script": "x", "product": "abi"}, "p")

        assert isinstance(payload.config, _ProductPayloadConfig)
        assert payload.config.product == "abi"

    def test_base_payload_rejects_the_subclass_field(self, service: MagicMock) -> None:
        with pytest.raises(ValidationError, match="product"):
            BashPayload(service, {"script": "x", "product": "abi"}, "p")

    def test_hydration_keeps_the_subclass_fields(self, service: MagicMock) -> None:
        built = _ProductPayload(service, {"script": "x", "product": "abi"}, "p")
        spec = built.to_job_spec(_job())

        hydrated = _ProductPayload.from_job_spec(spec, service)

        assert isinstance(hydrated.config, _ProductPayloadConfig)
        assert hydrated.config.product == "abi"

    def test_lower_representation_drops_keys_it_does_not_define(
        self,
        service: MagicMock,
    ) -> None:
        built = _ProductPayload(service, {"script": "x", "product": "abi"}, "p")
        spec = built.to_job_spec(_job())

        lowered = BashPayload.from_job_spec(spec, service)

        assert type(lowered.config) is BashPayloadConfig
        assert lowered.config.script == "x"

    def test_hydration_still_validates_types(self, service: MagicMock) -> None:
        spec = PayloadSpec(
            name="bash_payload",
            identifier="p",
            config={"script": "x", "prefix_args": "not-a-list"},
        )

        with pytest.raises(ValidationError):
            BashPayload.from_job_spec(spec, service)

    def test_base_config_defaults_on_both_paths(self, service: MagicMock) -> None:
        built = BashPayload(service, {"script": "x"}, "p")
        hydrated = BashPayload.from_job_spec(built.to_job_spec(_job()), service)

        assert built.base_config == DispatcherGroupConfig()
        assert hydrated.base_config == DispatcherGroupConfig()

    def test_from_job_spec_attaches_the_dispatcher_config(
        self,
        service: MagicMock,
    ) -> None:
        base = DispatcherGroupConfig(timeout_seconds=5)
        spec = BashPayload(service, {"script": "x"}, "p").to_job_spec(_job())

        assert BashPayload.from_job_spec(spec, service, base).base_config is base

    def test_default_base_config_lets_a_built_payload_probe(
        self,
        service: MagicMock,
    ) -> None:
        payload = ShellPayload(service, {"script": "x"}, "p")

        assert payload.validate_toolchain_arg("sh")[0].return_code == 0

    def test_payload_name_is_the_configured_plugin(self, service: MagicMock) -> None:
        built = _ProductPayload(service, {"script": "x"}, "p")
        lowered = BashPayload.from_job_spec(built.to_job_spec(_job()), service)

        assert built.payload_name == "product_payload"
        assert lowered.name == "bash_payload"
        assert lowered.payload_name == "product_payload"

    def test_get_metrics_is_labelled_with_the_payload_name(
        self,
        service: MagicMock,
    ) -> None:
        spec = PayloadSpec(
            name="labelled_payload",
            identifier="p-metrics",
            script="x",
        )
        lowered = BashPayload.from_job_spec(spec, service)
        PAYLOAD_JOBS_PROCESSED.labels(
            status="success",
            payload_name="labelled_payload",
            payload_identifier="p-metrics",
        ).inc()

        metrics = lowered.get_metrics()

        assert metrics
        assert {m["labels"]["payload_name"] for m in metrics.values()} == {
            "labelled_payload",
        }


class _RecordingPayload(ShellPayload):
    """Records the keyword arguments of every command it is asked to run."""

    calls: ClassVar[list[dict[str, Any]]] = []

    def get_payload_from_job(
        self,
        command: list[str],
        job: Job | None = None,
        log_prefix: str = "",
        log_file_path: Path | None = None,
        *,
        probe: bool = False,
    ) -> list[ExecutionLog]:
        self.calls.append(
            {
                "command": command,
                "job": job,
                "log_file_path": log_file_path,
                "probe": probe,
            },
        )
        return [ExecutionLog(return_code=0)]


def test_toolchain_probe_is_marked_as_a_probe(service: MagicMock) -> None:
    _RecordingPayload.calls.clear()
    payload = _RecordingPayload(service, {"script": "x", "toolchain": ["sh"]}, "p")

    payload.validate_toolchain_arg("sh")

    assert _RecordingPayload.calls == [
        {
            "command": _RecordingPayload.calls[0]["command"],
            "job": None,
            "log_file_path": None,
            "probe": True,
        },
    ]


def test_base_get_payload_from_job_accepts_probe(service: MagicMock) -> None:
    assert Payload.get_payload_from_job(
        BashPayload(service, {"script": "x"}, "p"),
        ["true"],
        probe=True,
    ) == [ExecutionLog()]


# ── config hygiene: unknown, removed and misplaced keys ─────────────────────


class TestDispatcherConfigKeys:
    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DispatcherGroupConfig.model_validate({"timeout_secnds": 5})

    @pytest.mark.parametrize(
        ("key", "advice"),
        [
            ("bash_script", "payload block"),
            ("max_workers", "dispatcher replicas"),
            ("fail_fast", "parallel_bash was removed"),
            ("python_venv", "default_binary"),
            ("falcon", "nested `payload:` block"),
            ("falconer", "falconers were replaced by dispatchers"),
            ("sbatch_template", "#SBATCH"),
        ],
    )
    @pytest.mark.parametrize(
        "model",
        [DispatcherGroupConfig, LocalDispatcherConfig, SlurmDispatcherConfig],
        ids=lambda model: model.__name__,
    )
    def test_removed_key_fails_with_advice(
        self,
        model: type[DispatcherGroupConfig],
        key: str,
        advice: str,
        tmp_path: Path,
    ) -> None:
        config: dict[str, Any] = {key: "anything"}
        if model is SlurmDispatcherConfig:
            config["slurm_output_dir"] = str(tmp_path)

        with pytest.raises(ValidationError) as info:
            model.model_validate(config)

        message = str(info.value)
        assert f"{key!r} is no longer supported" in message
        assert advice in message

    def test_every_removed_key_is_reported_at_once(self) -> None:
        with pytest.raises(ValidationError) as info:
            DispatcherGroupConfig.model_validate(
                {"bash_script": "echo", "max_workers": 4, "python_venv": "/v"},
            )

        for key in ("bash_script", "max_workers", "python_venv"):
            assert repr(key) in str(info.value)

    def test_python_venv_advice_does_not_suggest_toolchain_prepend(self) -> None:
        advice = REMOVED_DISPATCHER_KEYS["python_venv"]

        assert "does not do this" in advice
        assert "activate" in advice

    def test_log_dir_that_cannot_be_created_is_a_validation_error(
        self,
        tmp_path: Path,
    ) -> None:
        blocker = tmp_path / "a-file"
        blocker.write_text("")

        with pytest.raises(ValidationError, match="log_dir cannot be created"):
            DispatcherGroupConfig.model_validate(
                {"log_to_file": True, "log_dir": str(blocker / "logs")},
            )

    def test_log_dir_that_is_a_file_is_a_validation_error(
        self,
        tmp_path: Path,
    ) -> None:
        blocker = tmp_path / "a-file"
        blocker.write_text("")

        with pytest.raises(ValidationError, match="log_dir is not a directory"):
            DispatcherGroupConfig.model_validate(
                {"log_to_file": True, "log_dir": str(blocker)},
            )

    def test_offline_validation_does_not_touch_the_log_dir(
        self,
        tmp_path: Path,
    ) -> None:
        log_dir = tmp_path / "not" / "yet"

        DispatcherGroupConfig.model_validate(
            {"log_to_file": True, "log_dir": str(log_dir)},
            context={"offline": True},
        )

        assert not (tmp_path / "not").exists()

    @pytest.mark.parametrize(
        "model",
        [DispatcherGroupConfig, LocalDispatcherConfig],
        ids=lambda model: model.__name__,
    )
    def test_payload_setting_in_a_dispatcher_block_says_where_it_goes(
        self,
        model: type[DispatcherGroupConfig],
    ) -> None:
        """A half-migration (``bash_script`` renamed ``script``) gets a pointer."""
        with pytest.raises(ValidationError) as info:
            model.model_validate({"script": "echo", "prefix_args": ["-x"]})

        message = str(info.value)
        assert "'script', 'prefix_args': payload setting(s)" in message
        assert "job builder's nested payload block" in message


class TestPayloadConfigKeys:
    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            PayloadConfig.model_validate({"script": "x", "scirpt": "y"})

    def test_subclass_inherits_the_rejection(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BashPayloadConfig.model_validate({"script": "x", "bogus": 1})

    def test_hydrating_context_still_rejects_unknown_keys(self) -> None:
        # ``courier validate`` checks payload blocks with this context.
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            PayloadConfig.model_validate(
                {"script": "x", "bogus": 1},
                context={"hydrating": True},
            )

    def test_hydrating_context_skips_the_template_existence_check(self) -> None:
        config = PayloadConfig.model_validate(
            {"file": "/nowhere/template.sh"},
            context={"hydrating": True},
        )

        assert config.file == Path("/nowhere/template.sh")

    def test_dispatcher_setting_in_a_payload_block_says_where_it_goes(self) -> None:
        with pytest.raises(ValidationError, match="move them to the dispatcher"):
            PayloadConfig.model_validate({"script": "x", "timeout_seconds": 5})

    @pytest.mark.parametrize(
        ("key", "advice"),
        [("bash_script", "`script:`"), ("python_venv", "default_binary")],
    )
    def test_removed_key_in_a_payload_block_fails_with_advice(
        self,
        key: str,
        advice: str,
    ) -> None:
        with pytest.raises(ValidationError) as info:
            PayloadConfig.model_validate({"script": "x", key: "y"})

        assert f"{key!r} is no longer supported" in str(info.value)
        assert advice in str(info.value)

    def test_missing_source_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="'file', 'script', or 'binary'"):
            PayloadConfig.model_validate({})


# ── misplaced keys: derived from every installed plugin's config model ─────


class _EntryPoint:
    """Stands in for an installed entry point whose ``load`` is *loader*."""

    def __init__(self, name: str, loader: Callable[[], type]) -> None:
        self.name = name
        self.value = f"tests:{name}"
        self.load = loader


def _install(
    monkeypatch: pytest.MonkeyPatch,
    group: str,
    **loaders: Callable[[], type],
) -> None:
    """Make *loaders* look like plugins installed in entry-point *group*."""
    real = discovery._entry_points

    def _entry_points(requested: str) -> dict[str, Any]:
        found: dict[str, Any] = dict(real(requested))
        if requested == group:
            found.update(
                {name: _EntryPoint(name, load) for name, load in loaders.items()},
            )
        return found

    monkeypatch.setattr(discovery, "_entry_points", _entry_points)


def _installed_fields(registry: Any) -> list[tuple[str, str]]:
    """Return ``(plugin name, field)`` for every installed plugin's config."""
    return [
        (name, field)
        for name in registry.names()
        for field in registry.get_plugin(name).config_class.model_fields
    ]


_DISPATCHER_FIELDS = [
    pytest.param(name, field, id=f"{name}.{field}")
    for name, field in _installed_fields(dispatchers)
    if field not in PayloadConfig.model_fields
]
_PAYLOAD_FIELDS = [
    pytest.param(name, field, id=f"{name}.{field}")
    for name, field in _installed_fields(payloads)
    if field not in DispatcherGroupConfig.model_fields
]


#: The base model and every installed dispatcher's config model.
_DISPATCHER_MODELS: list[type[DispatcherGroupConfig]] = sorted(
    {
        DispatcherGroupConfig,
        *(
            cast("type[Dispatcher]", dispatchers.get_plugin(name)).config_class
            for name in dispatchers.names()
        ),
    },
    key=lambda model: model.__name__,
)


def test_the_misplaced_key_cases_cover_the_slurm_options() -> None:
    """Guard the guard: the derived cases include dispatcher-specific options."""
    fields = {param.values[1] for param in _DISPATCHER_FIELDS}

    assert {"partition", "slurm_output_dir", "wait_for_completion"} <= fields
    assert {"timeout_seconds", "log_dir", "output_files"} <= fields
    assert {LocalDispatcherConfig, SlurmDispatcherConfig} <= set(_DISPATCHER_MODELS)


class TestMisplacedKeys:
    """A setting of the other kind of block is explained, whichever plugin has it."""

    @pytest.mark.parametrize(("dispatcher", "field"), _DISPATCHER_FIELDS)
    def test_every_dispatcher_setting_in_a_payload_block_is_explained(
        self,
        service: MagicMock,
        dispatcher: str,
        field: str,
    ) -> None:
        """``courier run`` builds the payload, which validates with this advice."""
        with pytest.raises(ValidationError) as info:
            BashPayload(service, {"script": "x", field: "anything"}, "p")

        message = str(info.value)
        assert f"{field!r}" in message
        assert "dispatcher option(s) set in a payload block" in message
        assert "move them to the dispatcher's config" in message
        if field not in DispatcherGroupConfig.model_fields:
            assert f"{field!r} ({dispatcher})" in message

    @pytest.mark.parametrize(("payload", "field"), _PAYLOAD_FIELDS)
    @pytest.mark.parametrize(
        "model",
        _DISPATCHER_MODELS,
        ids=lambda model: model.__name__,
    )
    def test_every_payload_setting_in_a_dispatcher_block_is_explained(
        self,
        model: type[DispatcherGroupConfig],
        payload: str,
        field: str,
        tmp_path: Path,
    ) -> None:
        del payload
        config: dict[str, Any] = {field: "anything"}
        if model is SlurmDispatcherConfig:
            config["slurm_output_dir"] = str(tmp_path)

        with pytest.raises(ValidationError) as info:
            model.model_validate(config)

        message = str(info.value)
        assert f"{field!r}: payload setting(s) set in a dispatcher block" in message

    def test_slurm_options_name_the_dispatcher_that_takes_them(self) -> None:
        with pytest.raises(ValidationError) as info:
            PayloadConfig.model_validate(
                {"script": "x", "partition": "gpu", "timeout_seconds": 5},
            )

        assert (
            "'partition' (slurm_dispatcher), 'timeout_seconds': dispatcher "
            "option(s) set in a payload block"
        ) in str(info.value)

    def test_a_third_party_dispatchers_option_is_recognised(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The key sets come from the installed models, not from a hand list."""

        class _PriorityDispatcher(Dispatcher):
            name = "priority_dispatcher"
            config_class: ClassVar[type[DispatcherGroupConfig]] = create_model(
                "_PriorityConfig",
                __base__=DispatcherGroupConfig,
                priority=(int, 0),
            )

        _install(
            monkeypatch,
            dispatchers.group,
            priority_dispatcher=lambda: _PriorityDispatcher,
        )

        with pytest.raises(ValidationError) as info:
            PayloadConfig.model_validate({"script": "x", "priority": 1})

        assert "'priority' (priority_dispatcher)" in str(info.value)

    def test_a_third_party_payloads_option_is_recognised(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _VenvPayload(ShellPayload):
            name = "venv_payload"
            config_class: ClassVar[type[PayloadConfig]] = create_model(
                "_VenvConfig",
                __base__=PayloadConfig,
                venv=(str | None, None),
            )

        _install(monkeypatch, payloads.group, venv_payload=lambda: _VenvPayload)

        with pytest.raises(ValidationError) as info:
            LocalDispatcherConfig.model_validate({"venv": "/opt/venv"})

        assert "'venv' (venv_payload): payload setting(s)" in str(info.value)

    def test_a_plugin_that_cannot_load_is_skipped(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _broken() -> type:
            raise ImportError("missing optional dependency")

        _install(monkeypatch, dispatchers.group, broken_dispatcher=_broken)

        with pytest.raises(ValidationError) as info:
            PayloadConfig.model_validate({"script": "x", "partition": "gpu"})

        assert "'partition' (slurm_dispatcher)" in str(info.value)

    def test_a_valid_block_looks_up_no_plugin(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _refuse(_group: str) -> Any:
            raise AssertionError("a valid block looked up the installed plugins")

        monkeypatch.setattr(discovery, "_entry_points", _refuse)

        PayloadConfig.model_validate({"script": "x", "prefix_args": ["-e"]})
        DispatcherGroupConfig.model_validate({"timeout_seconds": 5})


def _plugin_blocks(node: Any) -> list[dict[str, Any]]:
    """Return every ``{kind, name, config}`` plugin block nested in *node*."""
    blocks: list[dict[str, Any]] = []
    if isinstance(node, dict):
        if isinstance(node.get("kind"), str) and "name" in node:
            blocks.append(node)
        for value in node.values():
            blocks.extend(_plugin_blocks(value))
    elif isinstance(node, list):
        for value in node:
            blocks.extend(_plugin_blocks(value))
    return blocks


_SHIPPED = sorted(
    path
    for path in [_REPO_ROOT / "config.yaml", *(_REPO_ROOT / "tests").glob("*.yaml")]
    if "docker-compose" not in path.name
)
_REGISTRIES = {"dispatcher": dispatchers, "payload": payloads}


def _checked_blocks(path: Path) -> list[dict[str, Any]]:
    """Return the dispatcher and payload blocks of the config at *path*."""
    return [
        block
        for block in _plugin_blocks(yaml.safe_load(path.read_text()))
        if str(block["kind"]).replace("-", "_").lower() in _REGISTRIES
    ]


def test_shipped_configs_have_blocks_to_check() -> None:
    """Guard the guard: an empty walk would make the test below vacuous."""
    assert sum(len(_checked_blocks(path)) for path in _SHIPPED) >= 4


@pytest.mark.parametrize("path", _SHIPPED, ids=lambda path: path.name)
def test_shipped_dispatcher_and_payload_blocks_validate(path: Path) -> None:
    """Every shipped dispatcher and payload block passes its plugin's model."""
    for block in _checked_blocks(path):
        registry = _REGISTRIES[str(block["kind"]).replace("-", "_").lower()]
        plugin_class = registry.get_plugin(block["name"])
        # Template paths are relative to wherever ``courier run`` starts, so
        # the on-disk check is skipped; every key is still checked.
        plugin_class.config_class.model_validate(
            block.get("config") or {},
            context={"hydrating": True},
        )


# ── write_script: TMPDIR, exclusive creation, mode ──────────────────────────


class TestWriteScript:
    def test_temp_script_honours_tmpdir(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        monkeypatch.setattr(tempfile, "tempdir", None)
        payload = BashPayload(service, {"script": "x"}, "p")

        path = payload.write_script("echo hi\n")

        assert path.parent == tmp_path
        assert path.suffix == ".sh"
        assert path.read_text() == "echo hi\n"
        assert stat.S_IMODE(path.stat().st_mode) == 0o755

    def test_explicit_path_is_created_with_parents(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        payload = BashPayload(service, {"script": "x"}, "p")
        target = tmp_path / "nested" / "dir" / "job.sh"

        path = payload.write_script("echo hi\n", target)

        assert path == target
        assert target.read_text() == "echo hi\n"
        assert stat.S_IMODE(target.stat().st_mode) == 0o755

    def test_existing_file_is_never_overwritten(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        payload = BashPayload(service, {"script": "x"}, "p")
        target = tmp_path / "job.sh"
        target.write_text("theirs")

        with pytest.raises(FileExistsError):
            payload.write_script("mine", target)

        assert target.read_text() == "theirs"

    def test_symlink_is_never_followed(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        payload = BashPayload(service, {"script": "x"}, "p")
        victim = tmp_path / "victim"
        victim.write_text("precious")
        victim.chmod(0o600)
        link = tmp_path / "job.sh"
        link.symlink_to(victim)

        with pytest.raises(FileExistsError):
            payload.write_script("mine", link)

        assert victim.read_text() == "precious"
        assert stat.S_IMODE(victim.stat().st_mode) == 0o600

    def test_dangling_symlink_is_never_followed(
        self,
        service: MagicMock,
        tmp_path: Path,
    ) -> None:
        payload = BashPayload(service, {"script": "x"}, "p")
        victim = tmp_path / "would-be-created"
        link = tmp_path / "job.sh"
        link.symlink_to(victim)

        with pytest.raises(FileExistsError):
            payload.write_script("mine", link)

        assert not victim.exists()

    @pytest.mark.parametrize("explicit", [False, True], ids=["temp", "explicit"])
    def test_failed_write_leaves_no_file_behind(
        self,
        service: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        explicit: bool,  # noqa: FBT001
    ) -> None:
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        monkeypatch.setattr(tempfile, "tempdir", None)
        payload = BashPayload(service, {"script": "x"}, "p")

        def fail(*_args: object) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(os, "fchmod", fail)

        with pytest.raises(OSError, match="disk full"):
            payload.write_script("echo hi\n", tmp_path / "job.sh" if explicit else None)

        assert list(tmp_path.iterdir()) == []
