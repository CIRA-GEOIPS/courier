"""Unit tests for courier.cli.init_helpers."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from courier.cli.init_helpers import (
    _module_config_model,
    find_config_model,
    get_field_metadata,
    get_plugin_description,
)
from courier.interfaces.payloads import Payload, PayloadConfig
from courier.plugins.data_monitors.s3_poller import S3Poller, S3PollerConfig
from courier.plugins.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
    FileSystemPollerConfig,
)
from courier.plugins.data_monitors.rabbit_mq_watcher import (
    RabbitMQWatcher,
    RabbitMQWatcherConfig,
)
from courier.plugins.job_builders.dummy_job_builder import (
    DummyJobBuilder,
    DummyJobBuilderConfig,
)
from courier.plugins.job_builders.metadata_router import (
    MetadataRouterBuilder,
    MetadataRouterConfig,
)
from courier.plugins.dispatchers.local_dispatcher import (
    LocalDispatcher,
    LocalDispatcherConfig,
)
from courier.plugins.dispatchers.slurm_dispatcher import (
    SlurmDispatcher,
    SlurmDispatcherConfig,
)
from courier.plugins.payloads.bash_payload import BashPayload, BashPayloadConfig
from courier.plugins.payloads.python_payload import (
    PythonPayload,
    PythonPayloadConfig,
)
from courier.plugins.payloads.shell_payload import ShellPayload, ShellPayloadConfig


class _OddSettings(PayloadConfig):
    """What ``_OddPayload`` really validates with."""

    extra_option: str = ""


class _OddPayloadConfig(BaseModel):
    """Named like ``_OddPayload``'s companion, but not what it validates with."""

    unrelated: int = 0


class _OddPayload(Payload):
    config_class = _OddSettings


class TestFindConfigModel:
    """Tests for find_config_model()."""

    @pytest.mark.parametrize(
        "plugin_class, expected_config_class",
        [
            (S3Poller, S3PollerConfig),
            (FileSystemPoller, FileSystemPollerConfig),
            (RabbitMQWatcher, RabbitMQWatcherConfig),
            (DummyJobBuilder, DummyJobBuilderConfig),
            (MetadataRouterBuilder, MetadataRouterConfig),
            (LocalDispatcher, LocalDispatcherConfig),
            (SlurmDispatcher, SlurmDispatcherConfig),
            (BashPayload, BashPayloadConfig),
            (PythonPayload, PythonPayloadConfig),
            (ShellPayload, ShellPayloadConfig),
        ],
    )
    def test_finds_known_configs(self, plugin_class, expected_config_class):
        """All known plugins should find their companion Config models."""
        result = find_config_model(plugin_class)
        assert result is expected_config_class, (
            f"Expected {expected_config_class.__name__} for "
            f"{plugin_class.__name__}, got {result}"
        )

    def test_a_declared_config_class_wins_over_the_name_guess(self):
        """``config_class`` is what the plugin validates with, so init prompts
        for exactly those fields -- even when a model in the module has the
        name the guess looks for."""
        guessed = _module_config_model(_OddPayload)
        assert guessed is _OddPayloadConfig, "the name guess is not being tested"
        assert _OddPayloadConfig().unrelated == 0

        found = find_config_model(_OddPayload)

        assert found is _OddSettings
        assert _OddSettings(binary="true").extra_option == ""


class TestGetFieldMetadata:
    """Tests for get_field_metadata()."""

    def test_a_factory_default_is_not_offered_as_a_value(self):
        """``Field(default_factory=list)`` has no ``default``: pydantic reports
        ``PydanticUndefined``, which the prompt once offered as the default and
        wrote into the config as the string "PydanticUndefined"."""
        fields = get_field_metadata(PayloadConfig)
        toolchain = next(f for f in fields if f["name"] == "toolchain")

        assert toolchain["required"] is False
        assert toolchain["default"] is ...

    def test_required_field(self):
        """Required fields should have required=True."""
        fields = get_field_metadata(S3PollerConfig)
        bucket = next(f for f in fields if f["name"] == "bucket")
        assert bucket["required"] is True
        assert bucket["type_hint"] == "str"

    def test_optional_field(self):
        """Optional fields with defaults should have required=False."""
        fields = get_field_metadata(S3PollerConfig)
        region = next(f for f in fields if f["name"] == "region")
        assert region["required"] is False
        assert region["default"] == "us-east-1"

    def test_all_fields_present(self):
        """All model fields should be returned."""
        fields = get_field_metadata(FileSystemPollerConfig)
        names = {f["name"] for f in fields}
        assert "path" in names
        assert "hostname" in names

    def test_description_present(self):
        """Fields with description should have it in metadata."""
        fields = get_field_metadata(FileSystemPollerConfig)
        path_field = next(f for f in fields if f["name"] == "path")
        assert path_field["description"]


class TestGetPluginDescription:
    """Tests for get_plugin_description()."""

    def test_returns_string(self):
        """Should return a non-empty string for documented plugins."""
        desc = get_plugin_description(S3Poller)
        assert isinstance(desc, str)
        assert len(desc) > 0

    def test_first_sentence_only(self):
        """Should return only the first sentence."""
        desc = get_plugin_description(FileSystemPoller)
        # Should end with period or be one line
        assert "." in desc or "\n" not in desc
