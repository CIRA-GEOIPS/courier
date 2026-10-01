"""Payload construction and config hygiene.

Covers the builder-side contract of :class:`~courier.interfaces.payloads.Payload`
that does not involve deferred values: the template is read and compiled once
at construction, configs are validated by ``config_class`` on both
construction paths, and removed or misplaced keys fail with advice.
"""

# cspell:ignore secnds scirpt geteuid

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.interfaces import discovery
from courier.interfaces.payloads import (
    REMOVED_DISPATCHER_KEYS,
    DispatcherGroupConfig,
    Payload,
    PayloadConfig,
)
from courier.plugins.dispatchers.local_dispatcher import LocalDispatcherConfig
from courier.plugins.dispatchers.slurm_dispatcher import SlurmDispatcherConfig
from courier.plugins.payloads.bash_payload import BashPayload, BashPayloadConfig
from courier.plugins.payloads.shell_payload import ShellPayload
from courier.types.execution_log import ExecutionLog
from courier.types.file import File
from courier.types.job import Job
from courier.types.payload import PayloadSpec

#: Readable and searchable, but not writable, by its owner.
_READ_ONLY_DIR_MODE = 0o500


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

    @pytest.mark.parametrize("key", sorted(REMOVED_DISPATCHER_KEYS))
    def test_removed_key_fails_with_a_pointer_to_the_upgrade_guide(
        self,
        key: str,
    ) -> None:
        with pytest.raises(ValidationError) as info:
            DispatcherGroupConfig.model_validate({key: "anything"})

        message = str(info.value)
        assert f"{key!r} is no longer supported: {REMOVED_DISPATCHER_KEYS[key]}" in (
            message
        )
        assert "upgrade guide" in message

    @pytest.mark.parametrize(
        "model",
        [LocalDispatcherConfig, SlurmDispatcherConfig],
        ids=lambda model: model.__name__,
    )
    def test_every_dispatcher_rejects_removed_keys(
        self,
        model: type[DispatcherGroupConfig],
        tmp_path: Path,
    ) -> None:
        config: dict[str, Any] = {"bash_script": "echo"}
        if model is SlurmDispatcherConfig:
            config["slurm_output_dir"] = str(tmp_path)

        with pytest.raises(ValidationError, match="'bash_script' is no longer"):
            model.model_validate(config)

    def test_every_removed_key_is_reported_at_once(self) -> None:
        with pytest.raises(ValidationError) as info:
            DispatcherGroupConfig.model_validate(
                {"bash_script": "echo", "max_workers": 4, "python_venv": "/v"},
            )

        for key in ("bash_script", "max_workers", "python_venv"):
            assert repr(key) in str(info.value)

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

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can write any directory")
    def test_log_dir_that_is_not_writable_is_a_validation_error(
        self,
        tmp_path: Path,
    ) -> None:
        """The check leaves the directory's mode alone."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir(mode=_READ_ONLY_DIR_MODE)
        try:
            with pytest.raises(ValidationError, match="log_dir is not writable"):
                DispatcherGroupConfig.model_validate(
                    {"log_to_file": True, "log_dir": str(log_dir)},
                )
            assert stat.S_IMODE(log_dir.stat().st_mode) == _READ_ONLY_DIR_MODE
        finally:
            log_dir.chmod(0o700)

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


class TestPayloadConfigKeys:
    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            PayloadConfig.model_validate({"script": "x", "scirpt": "y"})

    def test_subclass_inherits_the_rejection(self) -> None:
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BashPayloadConfig.model_validate({"script": "x", "bogus": 1})

    def test_removed_key_in_a_payload_block_fails_with_advice(self) -> None:
        with pytest.raises(ValidationError) as info:
            PayloadConfig.model_validate({"script": "x", "python_venv": "/v"})

        assert (
            f"'python_venv' is no longer supported: "
            f"{REMOVED_DISPATCHER_KEYS['python_venv']}"
        ) in str(info.value)

    def test_missing_source_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="'file', 'script', or 'binary'"):
            PayloadConfig.model_validate({})


# ── misplaced keys: a static rule over the two base models ──────────────────


@pytest.mark.parametrize(
    "model",
    [DispatcherGroupConfig, LocalDispatcherConfig],
    ids=lambda model: model.__name__,
)
def test_payload_setting_in_a_dispatcher_block_says_where_it_goes(
    model: type[DispatcherGroupConfig],
) -> None:
    """A half-migration (``bash_script`` renamed ``script``) gets a pointer."""
    with pytest.raises(ValidationError) as info:
        model.model_validate({"script": "echo", "prefix_args": ["-x"]})

    message = str(info.value)
    assert "'script', 'prefix_args': payload setting(s)" in message
    assert "job builder's nested payload block" in message


def test_dispatcher_setting_in_a_payload_block_says_where_it_goes() -> None:
    with pytest.raises(ValidationError) as info:
        PayloadConfig.model_validate({"script": "x", "timeout_seconds": 5})

    assert (
        "'timeout_seconds': dispatcher option(s) set in a payload block; move "
        "them to the dispatcher's config"
    ) in str(info.value)


def test_a_plugin_specific_key_gets_the_generic_error() -> None:
    """Only base-model settings are explained: slurm's ``partition`` is unknown."""
    with pytest.raises(ValidationError) as info:
        PayloadConfig.model_validate({"script": "x", "partition": "gpu"})

    assert "Extra inputs are not permitted" in str(info.value)
    assert "dispatcher option(s)" not in str(info.value)


def test_a_valid_block_looks_up_no_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(_group: str) -> Any:
        raise AssertionError("a valid block looked up the installed plugins")

    monkeypatch.setattr(discovery, "_entry_points", _refuse)

    PayloadConfig.model_validate({"script": "x", "prefix_args": ["-e"]})
    DispatcherGroupConfig.model_validate({"timeout_seconds": 5})
