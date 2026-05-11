"""Unit tests for courier.cli.init_helpers."""

from __future__ import annotations

import pytest

from courier.cli.init_helpers import (
    find_config_model,
    get_field_metadata,
    get_plugin_description,
)
from courier.plugins.classes.data_monitors.s3_poller import S3Poller, S3PollerConfig
from courier.plugins.classes.data_monitors.file_system_poller_watchdog import (
    FileSystemPoller,
    FileSystemPollerConfig,
)
from courier.plugins.classes.data_monitors.rabbit_mq_watcher import (
    RabbitMQWatcher,
    RabbitMQWatcherConfig,
)
from courier.plugins.classes.job_builders.dummy_job_builder import (
    DummyJobBuilder,
    DummyJobBuilderConfig,
)
from courier.plugins.classes.job_builders.metadata_router import (
    MetadataRouterBuilder,
    MetadataRouterConfig,
)
from courier.plugins.classes.dispatchers.serial_bash import (
    SerialBashDispatcher,
    SerialBashConfig,
)


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
            (SerialBashDispatcher, SerialBashConfig),
        ],
    )
    def test_finds_known_configs(self, plugin_class, expected_config_class):
        """All known plugins should find their companion Config models."""
        result = find_config_model(plugin_class)
        assert result is expected_config_class, (
            f"Expected {expected_config_class.__name__} for "
            f"{plugin_class.__name__}, got {result}"
        )


class TestGetFieldMetadata:
    """Tests for get_field_metadata()."""

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
