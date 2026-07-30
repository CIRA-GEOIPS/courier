"""Unit tests for the kafka_consumer data monitor plugin."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from courier.plugins.data_monitors.kafka_consumer import (
    KafkaConsumer,
    KafkaConsumerConfig,
)
from courier.types.file import File


def _make_config(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "bootstrap_servers": ["broker:9092"],
        "topic": "files",
        "group_id": "courier",
    }
    defaults.update(overrides)
    return defaults


# ─── Config Validation ──────────────────────────────────────────────────────


class TestKafkaConsumerConfig:
    def test_minimal_valid(self) -> None:
        cfg = KafkaConsumerConfig.model_validate(_make_config())
        assert cfg.topic == "files"
        assert cfg.auto_offset_reset == "latest"
        assert cfg.poll_timeout_seconds == 1.0

    def test_missing_topic_raises(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerConfig.model_validate(
                {"bootstrap_servers": ["b"], "group_id": "g"},
            )

    def test_empty_bootstrap_servers_raises(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerConfig.model_validate(
                {"bootstrap_servers": [], "topic": "t", "group_id": "g"},
            )

    def test_sasl_requires_credentials(self) -> None:
        with pytest.raises(ValidationError, match="sasl_mechanism requires"):
            KafkaConsumerConfig.model_validate(
                _make_config(sasl_mechanism="PLAIN"),
            )

    def test_sasl_with_credentials(self) -> None:
        cfg = KafkaConsumerConfig.model_validate(
            _make_config(
                sasl_mechanism="PLAIN",
                sasl_plain_username="u",
                sasl_plain_password="p",
            ),
        )
        assert cfg.sasl_mechanism == "PLAIN"

    def test_invalid_offset_reset_raises(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerConfig.model_validate(_make_config(auto_offset_reset="invalid"))

    def test_negative_poll_timeout_raises(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerConfig.model_validate(_make_config(poll_timeout_seconds=0))


# ─── Constructor ────────────────────────────────────────────────────────────


class TestConstructor:
    def test_initializes_with_defaults(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin.health is False
        assert plugin._listener_thread is None
        assert plugin.field_map["file"] == "file"

    def test_field_map_overrides(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(
            mock_service,
            _make_config(field_map={"file": "filepath"}),
        )
        assert plugin.field_map["file"] == "filepath"
        assert plugin.field_map["hostname"] == "hostname"


# ─── _decode_value ──────────────────────────────────────────────────────────


class TestDecodeValue:
    def test_none_returns_none(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._decode_value(None) is None

    def test_bytes_input_decodes(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._decode_value(b'{"k": 1}') == {"k": 1}

    def test_string_input_decodes(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._decode_value('{"k": 1}') == {"k": 1}

    def test_invalid_json_returns_none(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._decode_value(b"not json") is None

    def test_non_dict_returns_none(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._decode_value(b"[1, 2, 3]") is None


# ─── _message_to_file ───────────────────────────────────────────────────────


class TestMessageToFile:
    def test_missing_file_field_returns_none(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin._message_to_file({}) is None

    def test_complete_payload(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        result = plugin._message_to_file(
            {
                "file": "/data/x.nc",
                "hostname": "h1",
                "source": "goes16",
                "instrument": "abi",
                "timestamp": "2026-01-01T00:00:00",
            },
        )
        assert isinstance(result, File)
        assert str(result.file) == "/data/x.nc"
        assert result.hostname == "h1"
        assert result.source == "goes16"
        assert isinstance(result.timestamp, datetime)

    def test_field_map_translation(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(
            mock_service,
            _make_config(field_map={"file": "filepath", "source": "platform"}),
        )
        result = plugin._message_to_file({"filepath": "/a.nc", "platform": "goes17"})
        assert result is not None
        assert result.source == "goes17"


# ─── is_healthy / stop ──────────────────────────────────────────────────────


class TestLifecycle:
    def test_unhealthy_without_listener(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        assert plugin.is_healthy() is False

    def test_unhealthy_when_thread_dead(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        plugin.health = True
        dead_thread = MagicMock()
        dead_thread.is_alive.return_value = False
        plugin._listener_thread = dead_thread
        assert plugin.is_healthy() is False

    def test_healthy_when_thread_alive(self, mock_service: MagicMock) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        plugin.health = True
        live_thread = MagicMock()
        live_thread.is_alive.return_value = True
        plugin._listener_thread = live_thread
        assert plugin.is_healthy() is True

    def test_stop_sets_event(self, mock_service: MagicMock, mocker) -> None:
        plugin = KafkaConsumer(mock_service, _make_config())
        mocker.patch.object(KafkaConsumer.__bases__[0], "stop", return_value=None)
        plugin.stop()
        assert plugin._stop_event.is_set()
