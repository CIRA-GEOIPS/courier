"""Unit tests for new Pydantic Config models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from courier.plugins.classes.data_monitors.file_system_poller_watchdog import (
    FileSystemPollerConfig,
)
from courier.plugins.classes.data_monitors.rabbit_mq_watcher import (
    RabbitMQWatcherConfig,
)
from courier.plugins.classes.job_builders.dummy_job_builder import (
    DummyJobBuilderConfig,
)


class TestFileSystemPollerConfig:
    """Tests for FileSystemPollerConfig."""

    def test_requires_path(self):
        with pytest.raises(ValidationError):
            FileSystemPollerConfig()  # type: ignore[call-arg]

    def test_default_hostname(self):
        cfg = FileSystemPollerConfig(path="/tmp")
        assert cfg.hostname == "localhost"

    def test_custom_hostname(self):
        cfg = FileSystemPollerConfig(path="/tmp", hostname="myhost")
        assert cfg.hostname == "myhost"


class TestRabbitMQWatcherConfig:
    """Tests for RabbitMQWatcherConfig."""

    def test_defaults(self):
        cfg = RabbitMQWatcherConfig()
        assert cfg.rabbitmq_host == "localhost"
        assert cfg.rabbitmq_port == 5672
        assert cfg.rabbitmq_virtual_host == "/"
        assert cfg.rabbitmq_queue == "file_catalog"
        assert cfg.rabbitmq_username == "guest"
        assert cfg.rabbitmq_password == "guest"
        assert cfg.rabbitmq_prefetch_count == 1
        assert cfg.max_retries == -1
        assert cfg.retry_delay_seconds == 2.0
        assert cfg.retry_backoff_factor == 1.5
        assert cfg.field_map == {}

    def test_regex_requires_pattern(self):
        with pytest.raises(ValidationError):
            RabbitMQWatcherConfig(location_format="regex")

    def test_regex_with_pattern_works(self):
        cfg = RabbitMQWatcherConfig(
            location_format="regex",
            location_format_regex=r"(?P<hostname>.+)",
        )
        assert cfg.location_format_regex == r"(?P<hostname>.+)"

    def test_invalid_location_format(self):
        with pytest.raises(ValidationError):
            RabbitMQWatcherConfig(location_format="invalid_format")

    def test_custom_field_map(self):
        cfg = RabbitMQWatcherConfig(field_map={"location": "file_path"})
        assert cfg.field_map == {"location": "file_path"}

    def test_default_rate_limit(self):
        cfg = RabbitMQWatcherConfig()
        assert cfg.rate_limit_per_second == 0.0

    def test_custom_rate_limit(self):
        cfg = RabbitMQWatcherConfig(rate_limit_per_second=5.0)
        assert cfg.rate_limit_per_second == 5.0

    def test_negative_rate_limit_raises(self):
        with pytest.raises(ValidationError):
            RabbitMQWatcherConfig(rate_limit_per_second=-1.0)


class TestDummyJobBuilderConfig:
    """Tests for DummyJobBuilderConfig."""

    def test_defaults(self):
        cfg = DummyJobBuilderConfig()
        assert cfg.targets is None

    def test_with_targets(self):
        cfg = DummyJobBuilderConfig(targets=["t1", "t2"])
        assert cfg.targets == ["t1", "t2"]
